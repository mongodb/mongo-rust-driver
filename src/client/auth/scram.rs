use std::{
    borrow::Cow,
    collections::HashMap,
    fmt::{self, Display, Formatter},
    ops::{BitXor, Range},
    str,
    sync::RwLock,
};

use bson::{bson, doc, spec::BinarySubtype, Bson, Document};
use hmac::{Hmac, Mac};
use lazy_static::lazy_static;
use md5::Md5;
use sha1::{Digest, Sha1};
use sha2::Sha256;

use crate::{
    bson_util,
    client::auth::{self, Credential},
    cmap::{Command, Connection},
    error::{Error, Result},
};

// The single letter attribute keys in SCRAM messages.
const ITERATION_COUNT_KEY: char = 'i';
const ERROR_KEY: char = 'e';
const PROOF_KEY: char = 'p';
const VERIFIER_KEY: char = 'v';
const NONCE_KEY: char = 'r';
const SALT_KEY: char = 's';
const CHANNEL_BINDING_KEY: char = 'c';
const USERNAME_KEY: char = 'n';

/// Constant specifying that we won't be using channel binding.
const NO_CHANNEL_BINDING: char = 'n';

/// The minimum number of iterations of the hash function that we will accept from the server.
const MIN_ITERATION_COUNT: usize = 4096;

lazy_static! {
    /// Cache of pre-computed salted passwords.
    static ref CREDENTIAL_CACHE: RwLock<HashMap<CacheEntry, Vec<u8>>> = {
        RwLock::new(HashMap::new())
    };
}

#[derive(Hash, Eq, PartialEq)]
struct CacheEntry {
    password: String,
    salt: Vec<u8>,
    i: usize,
    mechanism: ScramVersion,
}

/// The versions of SCRAM supported by the driver (classified according to hash function used).
#[derive(Hash, Eq, PartialEq, Clone)]
pub(crate) enum ScramVersion {
    Sha1,
    Sha256,
}

impl ScramVersion {
    /// Perform SCRAM authentication for a given stream.
    pub(crate) fn authenticate_stream(
        &self,
        conn: &mut Connection,
        credential: &Credential,
    ) -> Result<()> {
        let username = credential
            .username
            .as_ref()
            .ok_or_else(|| Error::authentication_error("SCRAM", "no username supplied"))?;

        let password = credential
            .password
            .as_ref()
            .ok_or_else(|| Error::authentication_error("SCRAM", "no password supplied"))?;

        let source = match credential.source.as_ref() {
            Some(s) => s.as_str(),
            None => "admin",
        };

        if credential.mechanism_properties.is_some() {
            return Err(Error::authentication_error(
                "SCRAM",
                "mechanism properties MUST NOT be specified",
            ));
        };

        let nonce = auth::generate_nonce();

        let client_first = ClientFirst::new(username, nonce.as_str());

        let command = Command::new(
            "saslStart".into(),
            source.into(),
            client_first.to_command(self),
        );

        let server_first_response = conn.send_command(command)?;
        let server_first = ServerFirst::parse(server_first_response.raw_response)?;
        server_first.validate(nonce.as_str())?;

        let cache_entry_key = CacheEntry {
            password: password.to_string(),
            salt: server_first.salt().to_vec(),
            i: server_first.i(),
            mechanism: self.clone(),
        };
        let (should_update_cache, salted_password) =
            match CREDENTIAL_CACHE.read().unwrap().get(&cache_entry_key) {
                Some(pwd) => (false, pwd.clone()),
                None => (
                    true,
                    self.compute_salted_password(
                        username,
                        password,
                        server_first.i(),
                        server_first.salt(),
                    )?,
                ),
            };

        let client_final = ClientFinal::new(
            salted_password.as_slice(),
            &client_first,
            &server_first,
            self,
        )?;

        let command = Command::new(
            "saslContinue".into(),
            source.into(),
            client_final.to_command(),
        );

        let server_final_response = conn.send_command(command)?;
        let server_final = ServerFinal::parse(server_final_response.raw_response)?;
        server_final.validate(salted_password.as_slice(), &client_final, self)?;

        // Normal SCRAM implementations would cease here. The following round trip is MongoDB
        // implementation specific and just consists of a client no-op followed by a server no-op
        // with "done: true".
        let noop = doc! {
            "saslContinue": 1,
            "conversationId": server_final.conversation_id().clone(),
            "payload": Bson::Binary(BinarySubtype::Generic, Vec::new())
        };
        let command = Command::new("saslContinue".into(), source.into(), noop);

        let server_noop_response = conn.send_command(command)?;

        if server_noop_response
            .raw_response
            .get("conversationId")
            .map(|id| id == server_final.conversation_id())
            != Some(true)
        {
            return Err(Error::authentication_error(
                "SCRAM",
                "mismatched conversationId's",
            ));
        };

        if !server_noop_response
            .raw_response
            .get_bool("done")
            .unwrap_or(false)
        {
            return Err(Error::authentication_error(
                "SCRAM",
                "authentication did not complete successfully",
            ));
        }

        if should_update_cache {
            if let Ok(ref mut cache) = CREDENTIAL_CACHE.write() {
                if cache.get(&cache_entry_key).is_none() {
                    cache.insert(cache_entry_key, salted_password);
                }
            }
        }

        Ok(())
    }

    /// HMAC function used as part of SCRAM authentication.
    fn hmac(&self, key: &[u8], input: &[u8]) -> Result<Vec<u8>> {
        match self {
            ScramVersion::Sha1 => mac::<Hmac<Sha1>>(key, input),
            ScramVersion::Sha256 => mac::<Hmac<Sha256>>(key, input),
        }
    }

    /// Compute the HMAC of the given key and input and verify it matches the given signature.
    fn hmac_verify(&self, key: &[u8], input: &[u8], signature: &[u8]) -> Result<()> {
        match self {
            ScramVersion::Sha1 => mac_verify::<Hmac<Sha1>>(key, input, signature),
            ScramVersion::Sha256 => mac_verify::<Hmac<Sha256>>(key, input, signature),
        }
    }

    /// The "h" function defined in the SCRAM RFC.
    fn h(&self, str: &[u8]) -> Vec<u8> {
        match self {
            ScramVersion::Sha1 => hash::<Sha1>(str),
            ScramVersion::Sha256 => hash::<Sha256>(str),
        }
    }

    /// The "h_i" function as defined in the SCRAM RFC.
    fn h_i(&self, str: &str, salt: &[u8], iterations: usize) -> Vec<u8> {
        match self {
            ScramVersion::Sha1 => h_i::<Hmac<Sha1>>(str, salt, iterations, 160 / 8),
            ScramVersion::Sha256 => h_i::<Hmac<Sha256>>(str, salt, iterations, 256 / 8),
        }
    }

    /// Computes the salted password according to the SCRAM RFC and the MongoDB specific password
    /// hashing algorithm.
    fn compute_salted_password(
        &self,
        username: &str,
        password: &str,
        i: usize,
        salt: &[u8],
    ) -> Result<Vec<u8>> {
        let normalized_password = match self {
            ScramVersion::Sha1 => {
                let mut md5 = Md5::new();
                md5.input(format!("{}:mongo:{}", username, password));
                Cow::Owned(hex::encode(md5.result()))
            }
            ScramVersion::Sha256 => match stringprep::saslprep(password) {
                Ok(p) => p,
                Err(_) => {
                    return Err(Error::authentication_error(
                        "SCRAM-SHA-256",
                        "saslprep failure",
                    ))
                }
            },
        };

        Ok(self.h_i(normalized_password.as_ref(), salt, i))
    }
}

impl Display for ScramVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            ScramVersion::Sha1 => write!(f, "SCRAM-SHA-1"),
            ScramVersion::Sha256 => write!(f, "SCRAM-SHA-256"),
        }
    }
}

fn xor(lhs: &[u8], rhs: &[u8]) -> Vec<u8> {
    assert_eq!(lhs.len(), rhs.len());

    lhs.iter()
        .zip(rhs.iter())
        .map(|(l, r)| l.bitxor(r.clone()))
        .collect()
}

fn mac<M: Mac>(key: &[u8], input: &[u8]) -> Result<Vec<u8>> {
    let mut mac =
        M::new_varkey(key).or_else(|_| Err(Error::unknown_authentication_error("SCRAM")))?;
    mac.input(input);
    Ok(mac.result().code().to_vec())
}

fn mac_verify<M: Mac>(key: &[u8], input: &[u8], signature: &[u8]) -> Result<()> {
    let mut mac =
        M::new_varkey(key).or_else(|_| Err(Error::unknown_authentication_error("SCRAM")))?;
    mac.input(input);
    match mac.verify(signature) {
        Ok(_) => Ok(()),
        Err(_) => Err(Error::authentication_error(
            "SCRAM",
            "Authentication failed.",
        )),
    }
}

fn hash<D: Digest>(val: &[u8]) -> Vec<u8> {
    let mut hash = D::new();
    hash.input(val);
    hash.result().to_vec()
}

fn h_i<M: Mac + Sync>(str: &str, salt: &[u8], iterations: usize, output_size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; output_size];
    pbkdf2::pbkdf2::<M>(str.as_bytes(), salt, iterations, buf.as_mut_slice());
    buf
}

/// Parses a string slice of the form "<expected_key>=<body>" into "<body>", if possible.
fn parse_kvp(str: &str, expected_key: char) -> Result<String> {
    if str.chars().nth(0) != Some(expected_key) || str.chars().nth(1) != Some('=') {
        Err(Error::invalid_authentication_response("SCRAM"))
    } else {
        Ok(str.chars().skip(2).collect())
    }
}

fn validate_command_success(response: &Document) -> Result<()> {
    let ok = response
        .get("ok")
        .ok_or_else(|| Error::invalid_authentication_response("SCRAM"))?;
    match bson_util::get_int(ok) {
        Some(1) => Ok(()),
        Some(_) => Err(Error::authentication_error(
            "SCRAM",
            response
                .get_str("errmsg")
                .unwrap_or("Authentication failure"),
        )),
        _ => Err(Error::invalid_authentication_response("SCRAM")),
    }
}

/// Model of the first message sent by the client.
struct ClientFirst {
    message: String,

    gs2_header: Range<usize>,

    bare: Range<usize>,
}

impl ClientFirst {
    fn new(username: &str, nonce: &str) -> Self {
        let gs2_header = format!("{},,", NO_CHANNEL_BINDING);
        let bare = format!("{}={},{}={}", USERNAME_KEY, username, NONCE_KEY, nonce);
        let full = format!("{}{}", &gs2_header, &bare);
        let end = full.len();
        ClientFirst {
            message: full,
            gs2_header: Range {
                start: 0,
                end: gs2_header.len(),
            },
            bare: Range {
                start: gs2_header.len(),
                end,
            },
        }
    }

    fn bare_message(&self) -> &str {
        &self.message[self.bare.clone()]
    }

    fn gs2_header(&self) -> &str {
        &self.message[self.gs2_header.clone()]
    }

    fn message(&self) -> &str {
        &self.message[..]
    }

    fn to_command(&self, scram: &ScramVersion) -> Document {
        doc! {
            "saslStart": 1,
            "mechanism": scram.to_string(),
            "payload": Bson::Binary(BinarySubtype::Generic, self.message().as_bytes().to_vec())
        }
    }
}

/// Model of the first message received from the server.
///
/// This MUST be validated before sending the `ClientFinal` message back to the server.
struct ServerFirst {
    conversation_id: Bson,
    done: bool,
    message: String,
    nonce: String,
    salt: Vec<u8>,
    i: usize,
}

impl ServerFirst {
    fn parse(response: Document) -> Result<Self> {
        validate_command_success(&response)?;

        let conversation_id = response
            .get("conversationId")
            .ok_or_else(|| Error::authentication_error("SCRAM", "mismatched conversationId's"))?;
        let payload = match response.get_binary_generic("payload") {
            Ok(p) => p,
            Err(_) => return Err(Error::invalid_authentication_response("SCRAM")),
        };
        let done = response
            .get_bool("done")
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;
        let message = str::from_utf8(payload)
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;

        let parts: Vec<&str> = message.split(',').collect();

        if parts.len() < 3 {
            return Err(Error::invalid_authentication_response("SCRAM"));
        };

        let full_nonce = parse_kvp(parts[0], NONCE_KEY)?;

        let salt = base64::decode(parse_kvp(parts[1], SALT_KEY)?.as_str())
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;

        let i: usize = match parse_kvp(parts[2], ITERATION_COUNT_KEY)?.parse() {
            Ok(num) => num,
            Err(_) => {
                return Err(Error::authentication_error(
                    "SCRAM",
                    "iteration count invalid",
                ))
            }
        };

        Ok(ServerFirst {
            conversation_id: conversation_id.clone(),
            done,
            message: message.to_string(),
            nonce: full_nonce,
            salt,
            i,
        })
    }

    fn conversation_id(&self) -> &Bson {
        &self.conversation_id
    }

    fn message(&self) -> &str {
        self.message.as_str()
    }

    fn nonce(&self) -> &str {
        self.nonce.as_str()
    }

    fn salt(&self) -> &[u8] {
        self.salt.as_slice()
    }

    fn i(&self) -> usize {
        self.i
    }

    fn validate(&self, nonce: &str) -> Result<()> {
        if self.done {
            Err(Error::authentication_error(
                "SCRAM",
                "handshake terminated early",
            ))
        } else if &self.nonce[0..nonce.len()] != nonce {
            Err(Error::authentication_error("SCRAM", "mismatched nonce"))
        } else if self.i < MIN_ITERATION_COUNT {
            Err(Error::authentication_error(
                "SCRAM",
                "iteration count too low",
            ))
        } else {
            Ok(())
        }
    }
}

/// Model of the final message sent by the client.
///
/// Contains the "AuthMessage" mentioned in the RFC used in computing the client and server
/// signatures.
struct ClientFinal {
    message: String,
    auth_message: String,
    conversation_id: Bson,
}

impl ClientFinal {
    fn new(
        salted_password: &[u8],
        client_first: &ClientFirst,
        server_first: &ServerFirst,
        scram: &ScramVersion,
    ) -> Result<Self> {
        let client_key = scram.hmac(salted_password, b"Client Key")?;
        let stored_key = scram.h(client_key.as_slice());

        let without_proof = format!(
            "{}={},{}={}",
            CHANNEL_BINDING_KEY,
            base64::encode(client_first.gs2_header()),
            NONCE_KEY,
            server_first.nonce()
        );
        let auth_message = format!(
            "{},{},{}",
            client_first.bare_message(),
            server_first.message(),
            without_proof.as_str()
        );
        let client_signature = scram.hmac(stored_key.as_slice(), auth_message.as_bytes())?;
        let client_proof =
            base64::encode(xor(client_key.as_slice(), client_signature.as_slice()).as_slice());

        let message = format!("{},{}={}", without_proof, PROOF_KEY, client_proof);

        Ok(ClientFinal {
            message,
            auth_message,
            conversation_id: server_first.conversation_id().clone(),
        })
    }

    fn payload(&self) -> Bson {
        Bson::Binary(BinarySubtype::Generic, self.message().as_bytes().to_vec())
    }

    fn message(&self) -> &str {
        self.message.as_str()
    }

    fn auth_message(&self) -> &str {
        self.auth_message.as_str()
    }

    fn to_command(&self) -> Document {
        doc! {
            "saslContinue": 1,
            "conversationId": self.conversation_id.clone(),
            "payload": self.payload()
        }
    }
}

enum ServerFinalBody {
    Error(String),
    Verifier(String),
}

/// Model of the final message received from the server.
///
/// This MUST be validated before sending the final no-op message to the server.
struct ServerFinal {
    conversation_id: Bson,
    done: bool,
    body: ServerFinalBody,
}

impl ServerFinal {
    fn parse(response: Document) -> Result<Self> {
        validate_command_success(&response)?;

        let conversation_id = response
            .get("conversationId")
            .ok_or_else(|| Error::invalid_authentication_response("SCRAM"))?;
        let done = response
            .get_bool("done")
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;
        let payload = response
            .get_binary_generic("payload")
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;
        let message = str::from_utf8(payload)
            .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;

        let first = message
            .chars()
            .nth(0)
            .ok_or_else(|| Error::invalid_authentication_response("SCRAM"))?;
        let body = if first == ERROR_KEY {
            let error = parse_kvp(message, ERROR_KEY)?;
            ServerFinalBody::Error(error)
        } else if first == VERIFIER_KEY {
            let verifier = parse_kvp(message, VERIFIER_KEY)?;
            ServerFinalBody::Verifier(verifier)
        } else {
            return Err(Error::invalid_authentication_response("SCRAM"));
        };

        Ok(ServerFinal {
            conversation_id: conversation_id.clone(),
            done,
            body,
        })
    }

    fn validate(
        &self,
        salted_password: &[u8],
        client_final: &ClientFinal,
        scram: &ScramVersion,
    ) -> Result<()> {
        if self.done {
            return Err(Error::authentication_error(
                "SCRAM",
                "handshake terminated early",
            ));
        };

        if self.conversation_id != client_final.conversation_id {
            return Err(Error::authentication_error(
                "SCRAM",
                "mismatched conversationId's",
            ));
        };

        match self.body {
            ServerFinalBody::Verifier(ref body) => {
                let server_key = scram.hmac(salted_password, b"Server Key")?;
                let body_decoded = base64::decode(body.as_bytes())
                    .or_else(|_| Err(Error::invalid_authentication_response("SCRAM")))?;

                scram.hmac_verify(
                    server_key.as_slice(),
                    client_final.auth_message().as_bytes(),
                    body_decoded.as_slice(),
                )
            }
            ServerFinalBody::Error(ref err) => {
                Err(Error::authentication_error("SCRAM", err.as_str()))
            }
        }
    }

    fn conversation_id(&self) -> &Bson {
        &self.conversation_id
    }
}

#[cfg(test)]
mod tests {
    use bson::Bson;

    use crate::options::auth::scram::ServerFirst;

    #[test]
    fn test_iteration_count() {
        let nonce = "mocked";

        let invalid_iteration_count = ServerFirst {
            conversation_id: Bson::Null,
            done: false,
            message: "mocked".to_string(),
            nonce: nonce.to_string(),
            salt: Vec::new(),
            i: 42,
        };
        assert!(invalid_iteration_count.validate(nonce).is_err());

        let valid_iteration_count = ServerFirst {
            i: 4096,
            ..invalid_iteration_count
        };
        assert!(valid_iteration_count.validate(nonce).is_ok())
    }
}
