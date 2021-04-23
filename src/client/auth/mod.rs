//! Contains the types needed to specify the auth configuration for a
//! [`Client`](struct.Client.html).

#[cfg(feature = "tokio-runtime")]
mod aws;
mod plain;
mod sasl;
mod scram;
#[cfg(test)]
mod test;
mod x509;

use std::{borrow::Cow, str::FromStr};

use hmac::{Mac, NewMac};
use rand::Rng;
use serde::Deserialize;
use typed_builder::TypedBuilder;

use self::scram::ScramVersion;
use crate::{
    bson::Document,
    client::options::ServerApi,
    cmap::{Command, Connection, StreamDescription},
    error::{Error, ErrorKind, Result},
    runtime::HttpClient,
};

const SCRAM_SHA_1_STR: &str = "SCRAM-SHA-1";
const SCRAM_SHA_256_STR: &str = "SCRAM-SHA-256";
const MONGODB_CR_STR: &str = "MONGODB-CR";
const GSSAPI_STR: &str = "GSSAPI";
const MONGODB_AWS_STR: &str = "MONGODB-AWS";
const MONGODB_X509_STR: &str = "MONGODB-X509";
const PLAIN_STR: &str = "PLAIN";

/// The authentication mechanisms supported by MongoDB.
///
/// Note: not all of these mechanisms are currently supported by the driver.
#[derive(Clone, Deserialize, PartialEq, Debug)]
#[non_exhaustive]
pub enum AuthMechanism {
    /// MongoDB Challenge Response nonce and MD5 based authentication system. It is currently
    /// deprecated and will never be supported by this driver.
    MongoDbCr,

    /// The SCRAM-SHA-1 mechanism as defined in [RFC 5802](http://tools.ietf.org/html/rfc5802).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ScramSha1,

    /// The SCRAM-SHA-256 mechanism which extends [RFC 5802](http://tools.ietf.org/html/rfc5802) and is formally defined in [RFC 7677](https://tools.ietf.org/html/rfc7677).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-scram/) for more information.
    ScramSha256,

    /// The MONGODB-X509 mechanism based on the usage of X.509 certificates to validate a client
    /// where the distinguished subject name of the client certificate acts as the username.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-x.509/) for more information.
    MongoDbX509,

    /// Kerberos authentication mechanism as defined in [RFC 4752](http://tools.ietf.org/html/rfc4752).
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/kerberos/) for more information.
    ///
    /// Note: This mechanism is not currently supported by this driver but will be in the future.
    Gssapi,

    /// The SASL PLAIN mechanism, as defined in [RFC 4616](), is used in MongoDB to perform LDAP
    /// authentication and cannot be used for any other type of authenticaiton.
    /// Since the credentials are stored outside of MongoDB, the "$external" database must be used
    /// for authentication.
    ///
    /// See the [MongoDB documentation](https://docs.mongodb.com/manual/core/security-ldap/#ldap-proxy-authentication) for more information on LDAP authentication.
    Plain,

    /// MONGODB-AWS authenticates using AWS IAM credentials (an access key ID and a secret access
    /// key), temporary AWS IAM credentials obtained from an AWS Security Token Service (STS)
    /// Assume Role request, or temporary AWS IAM credentials assigned to an EC2 instance or ECS
    /// task.
    ///
    /// Note: Only server versions 4.4+ support AWS authentication. Additionally, the driver only
    /// supports AWS authentication with the tokio runtime.
    #[cfg(feature = "tokio-runtime")]
    MongoDbAws,
}

impl AuthMechanism {
    fn from_scram_version(scram: &ScramVersion) -> Self {
        match scram {
            ScramVersion::Sha1 => Self::ScramSha1,
            ScramVersion::Sha256 => Self::ScramSha256,
        }
    }

    pub(crate) fn from_stream_description(description: &StreamDescription) -> AuthMechanism {
        let scram_sha_256_found = description
            .sasl_supported_mechs
            .as_ref()
            .map(|ms| ms.iter().any(|m| m == AuthMechanism::ScramSha256.as_str()))
            .unwrap_or(false);

        if scram_sha_256_found {
            AuthMechanism::ScramSha256
        } else {
            AuthMechanism::ScramSha1
        }
    }

    /// Determines if the provided credentials have the required information to perform
    /// authentication.
    pub fn validate_credential(&self, credential: &Credential) -> Result<()> {
        match self {
            AuthMechanism::ScramSha1 | AuthMechanism::ScramSha256 => {
                if credential.username.is_none() {
                    return Err(ErrorKind::ArgumentError {
                        message: "No username provided for SCRAM authentication".to_string(),
                    }
                    .into());
                };
                Ok(())
            }
            AuthMechanism::MongoDbX509 => {
                if credential.password.is_some() {
                    return Err(ErrorKind::ArgumentError {
                        message: "A password cannot be specified with MONGODB-X509".to_string(),
                    }
                    .into());
                }

                if credential.source.as_deref().unwrap_or("$external") != "$external" {
                    return Err(ErrorKind::ArgumentError {
                        message: "only $external may be specified as an auth source for \
                                  MONGODB-X509"
                            .to_string(),
                    }
                    .into());
                }

                Ok(())
            }
            AuthMechanism::Plain => {
                if credential.username.is_none() {
                    return Err(ErrorKind::ArgumentError {
                        message: "No username provided for PLAIN authentication".to_string(),
                    }
                    .into());
                }

                if credential.username.as_deref() == Some("") {
                    return Err(ErrorKind::ArgumentError {
                        message: "Username for PLAIN authentication must be non-empty".to_string(),
                    }
                    .into());
                }

                if credential.password.is_none() {
                    return Err(ErrorKind::ArgumentError {
                        message: "No password provided for PLAIN authentication".to_string(),
                    }
                    .into());
                }

                Ok(())
            }
            #[cfg(feature = "tokio-runtime")]
            AuthMechanism::MongoDbAws => {
                if credential.username.is_some() && credential.password.is_none() {
                    return Err(ErrorKind::ArgumentError {
                        message: "Username cannot be provided without password for MONGODB-AWS \
                                  authentication"
                            .to_string(),
                    }
                    .into());
                }

                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Returns this `AuthMechanism` as a string.
    pub fn as_str(&self) -> &'static str {
        match self {
            AuthMechanism::ScramSha1 => SCRAM_SHA_1_STR,
            AuthMechanism::ScramSha256 => SCRAM_SHA_256_STR,
            AuthMechanism::MongoDbCr => MONGODB_CR_STR,
            AuthMechanism::MongoDbX509 => MONGODB_X509_STR,
            AuthMechanism::Gssapi => GSSAPI_STR,
            AuthMechanism::Plain => PLAIN_STR,
            #[cfg(feature = "tokio-runtime")]
            AuthMechanism::MongoDbAws => MONGODB_AWS_STR,
        }
    }

    /// Get the default authSource for a given mechanism depending on the database provided in the
    /// connection string.
    pub(crate) fn default_source<'a>(&'a self, uri_db: Option<&'a str>) -> &'a str {
        // TODO: fill in others as they're implemented
        match self {
            AuthMechanism::ScramSha1 | AuthMechanism::ScramSha256 | AuthMechanism::MongoDbCr => {
                uri_db.unwrap_or("admin")
            }
            AuthMechanism::MongoDbX509 => "$external",
            AuthMechanism::Plain => "$external",
            #[cfg(feature = "tokio-runtime")]
            AuthMechanism::MongoDbAws => "$external",
            _ => "",
        }
    }

    /// Constructs the first message to be sent to the server as part of the authentication
    /// handshake, which can be used for speculative authentication.
    pub(crate) fn build_speculative_client_first(
        &self,
        credential: &Credential,
    ) -> Result<Option<ClientFirst>> {
        match self {
            Self::ScramSha1 => {
                let client_first = ScramVersion::Sha1.build_speculative_client_first(credential)?;

                Ok(Some(ClientFirst::Scram(ScramVersion::Sha1, client_first)))
            }
            Self::ScramSha256 => {
                let client_first =
                    ScramVersion::Sha256.build_speculative_client_first(credential)?;

                Ok(Some(ClientFirst::Scram(ScramVersion::Sha256, client_first)))
            }
            Self::MongoDbX509 => Ok(Some(ClientFirst::X509(
                x509::build_speculative_client_first(credential),
            ))),
            Self::Plain => Ok(None),
            #[cfg(feature = "tokio-runtime")]
            AuthMechanism::MongoDbAws => Ok(None),
            AuthMechanism::MongoDbCr => Err(ErrorKind::AuthenticationError {
                message: "MONGODB-CR is deprecated and not supported by this driver. Use SCRAM \
                          for password-based authentication instead"
                    .into(),
            }
            .into()),
            _ => Err(ErrorKind::AuthenticationError {
                message: format!("Authentication mechanism {:?} not yet implemented.", self),
            }
            .into()),
        }
    }

    pub(crate) async fn authenticate_stream(
        &self,
        stream: &mut Connection,
        credential: &Credential,
        server_api: Option<&ServerApi>,
        #[cfg_attr(not(feature = "tokio-runtime"), allow(unused))] http_client: &HttpClient,
    ) -> Result<()> {
        self.validate_credential(credential)?;

        match self {
            AuthMechanism::ScramSha1 => {
                ScramVersion::Sha1
                    .authenticate_stream(stream, credential, server_api, None)
                    .await
            }
            AuthMechanism::ScramSha256 => {
                ScramVersion::Sha256
                    .authenticate_stream(stream, credential, server_api, None)
                    .await
            }
            AuthMechanism::MongoDbX509 => {
                x509::authenticate_stream(stream, credential, server_api, None).await
            }
            AuthMechanism::Plain => {
                plain::authenticate_stream(stream, credential, server_api).await
            }
            #[cfg(feature = "tokio-runtime")]
            AuthMechanism::MongoDbAws => {
                aws::authenticate_stream(stream, credential, server_api, http_client).await
            }
            AuthMechanism::MongoDbCr => Err(ErrorKind::AuthenticationError {
                message: "MONGODB-CR is deprecated and not supported by this driver. Use SCRAM \
                          for password-based authentication instead"
                    .into(),
            }
            .into()),
            _ => Err(ErrorKind::AuthenticationError {
                message: format!("Authentication mechanism {:?} not yet implemented.", self),
            }
            .into()),
        }
    }
}

impl FromStr for AuthMechanism {
    type Err = Error;

    fn from_str(str: &str) -> Result<Self> {
        match str {
            SCRAM_SHA_1_STR => Ok(AuthMechanism::ScramSha1),
            SCRAM_SHA_256_STR => Ok(AuthMechanism::ScramSha256),
            MONGODB_CR_STR => Ok(AuthMechanism::MongoDbCr),
            MONGODB_X509_STR => Ok(AuthMechanism::MongoDbX509),
            GSSAPI_STR => Ok(AuthMechanism::Gssapi),
            PLAIN_STR => Ok(AuthMechanism::Plain),

            #[cfg(feature = "tokio-runtime")]
            MONGODB_AWS_STR => Ok(AuthMechanism::MongoDbAws),
            #[cfg(not(feature = "tokio-runtime"))]
            MONGODB_AWS_STR => Err(ErrorKind::ArgumentError {
                message: "MONGODB-AWS auth is only supported with the tokio runtime".into(),
            }
            .into()),

            _ => Err(ErrorKind::ArgumentError {
                message: format!("invalid mechanism string: {}", str),
            }
            .into()),
        }
    }
}

/// A struct containing authentication information.
///
/// Some fields (mechanism and source) may be omitted and will either be negotiated or assigned a
/// default value, depending on the values of other fields in the credential.
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder, PartialEq)]
#[builder(field_defaults(default, setter(strip_option)))]
#[non_exhaustive]
pub struct Credential {
    /// The username to authenticate with. This applies to all mechanisms but may be omitted when
    /// authenticating via MONGODB-X509.
    pub username: Option<String>,

    /// The database used to authenticate. This applies to all mechanisms and defaults to "admin"
    /// in SCRAM authentication mechanisms and "$external" for GSSAPI, MONGODB-X509 and PLAIN.
    pub source: Option<String>,

    /// The password to authenticate with. This does not apply to all mechanisms.
    pub password: Option<String>,

    /// Which authentication mechanism to use. If not provided, one will be negotiated with the
    /// server.
    pub mechanism: Option<AuthMechanism>,

    /// Additional properties for the given mechanism.
    pub mechanism_properties: Option<Document>,
}

impl Credential {
    #[cfg(all(test, not(feature = "sync")))]
    pub(crate) fn into_document(mut self) -> Document {
        use crate::bson::Bson;

        let mut doc = Document::new();

        if let Some(s) = self.username.take() {
            doc.insert("username", s);
        }

        if let Some(s) = self.password.take() {
            doc.insert("password", s);
        } else {
            doc.insert("password", Bson::Null);
        }

        if let Some(s) = self.source.take() {
            doc.insert("db", s);
        }

        doc
    }

    pub(crate) fn resolved_source(&self) -> &str {
        self.mechanism
            .as_ref()
            .map(|m| m.default_source(None))
            .unwrap_or("admin")
    }

    /// If the mechanism is missing, append the appropriate mechanism negotiation key-value-pair to
    /// the provided isMaster command document.
    pub(crate) fn append_needed_mechanism_negotiation(&self, command: &mut Document) {
        if let (Some(username), None) = (self.username.as_ref(), self.mechanism.as_ref()) {
            command.insert(
                "saslSupportedMechs",
                format!("{}.{}", self.resolved_source(), username),
            );
        }
    }

    /// Attempts to authenticate a stream according this credential, returning an error
    /// result on failure. A mechanism may be negotiated if one is not provided as part of the
    /// credential.
    pub(crate) async fn authenticate_stream(
        &self,
        conn: &mut Connection,
        http_client: &HttpClient,
        server_api: Option<&ServerApi>,
        first_round: Option<FirstRound>,
    ) -> Result<()> {
        let stream_description = conn.stream_description()?;

        // Verify server can authenticate.
        if !stream_description.initial_server_type.can_auth() {
            return Ok(());
        };

        // If speculative authentication returned a response, then short-circuit the authentication
        // logic and use the first round from the handshake.
        if let Some(first_round) = first_round {
            return match first_round {
                FirstRound::Scram(version, first_round) => {
                    version
                        .authenticate_stream(conn, self, server_api, first_round)
                        .await
                }
                FirstRound::X509(server_first) => {
                    x509::authenticate_stream(conn, self, server_api, server_first).await
                }
            };
        }

        let mechanism = match self.mechanism {
            None => Cow::Owned(AuthMechanism::from_stream_description(stream_description)),
            Some(ref m) => Cow::Borrowed(m),
        };

        // Authenticate according to the chosen mechanism.
        mechanism
            .authenticate_stream(conn, self, server_api, http_client)
            .await
    }
}

/// Contains the first client message sent as part of the authentication handshake.
pub(crate) enum ClientFirst {
    Scram(ScramVersion, scram::ClientFirst),
    X509(Command),
}

impl ClientFirst {
    pub(crate) fn to_document(&self) -> Document {
        match self {
            Self::Scram(version, client_first) => client_first.to_command(&version).body,
            Self::X509(command) => command.body.clone(),
        }
    }

    pub(crate) fn into_first_round(self, server_first: Document) -> FirstRound {
        match self {
            Self::Scram(version, client_first) => FirstRound::Scram(
                version,
                scram::FirstRound {
                    client_first,
                    server_first,
                },
            ),
            Self::X509(..) => FirstRound::X509(server_first),
        }
    }
}

/// Contains the complete first round of the authentication handshake, including the client message
/// and the server response.
#[derive(Debug)]
pub(crate) enum FirstRound {
    Scram(ScramVersion, scram::FirstRound),
    X509(Document),
}

pub(crate) fn generate_nonce_bytes() -> [u8; 32] {
    rand::thread_rng().gen()
}

pub(crate) fn generate_nonce() -> String {
    let result = generate_nonce_bytes();
    base64::encode(&result)
}

fn mac<M: Mac + NewMac>(
    key: &[u8],
    input: &[u8],
    auth_mechanism: &str,
) -> Result<impl AsRef<[u8]>> {
    let mut mac =
        M::new_varkey(key).map_err(|_| Error::unknown_authentication_error(auth_mechanism))?;
    mac.update(input);
    Ok(mac.finalize().into_bytes())
}
