#[cfg(feature = "aws-auth")]
use aws_config::BehaviorVersion;

#[cfg(feature = "aws-auth")]
use aws_credential_types::{provider::ProvideCredentials, Credentials};

// Note from RUST-1529: commented Duration import since original implementation is commented out
// use std::time::Duration;
// use rand::distributions::{Alphanumeric, DistString};
// use std::{fs::File, io::Read};
// use crate::bson::rawdoc;

use chrono::{offset::Utc, DateTime};
use hmac::Hmac;
use once_cell::sync::Lazy;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    client::{
        auth::{
            self,
            sasl::{SaslContinue, SaslResponse, SaslStart},
            AuthMechanism,
            Credential,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
    runtime::HttpClient,
    serde_util,
};

#[cfg(not(feature = "bson-3"))]
use crate::bson_compat::DocumentExt as _;

// const AWS_ECS_IP: &str = "169.254.170.2";
// const AWS_EC2_IP: &str = "169.254.169.254";
const AWS_LONG_DATE_FMT: &str = "%Y%m%dT%H%M%SZ";
const MECH_NAME: &str = "MONGODB-AWS";

static CACHED_CREDENTIAL: Lazy<Mutex<Option<AwsCredential>>> = Lazy::new(|| Mutex::new(None));

/// Performs MONGODB-AWS authentication for a given stream.
pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    http_client: &HttpClient,
) -> Result<()> {
    match authenticate_stream_inner(conn, credential, server_api, http_client).await {
        Ok(()) => Ok(()),
        Err(error) => {
            *CACHED_CREDENTIAL.lock().await = None;
            Err(error)
        }
    }
}

async fn authenticate_stream_inner(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    // RUST-1529 note: http_client is used in the non-AWS SDK implementation to get credentials
    _http_client: &HttpClient,
) -> Result<()> {
    let source = match credential.source.as_deref() {
        Some("$external") | None => "$external",
        Some(..) => {
            return Err(Error::authentication_error(
                MECH_NAME,
                "auth source must be $external",
            ))
        }
    };

    let nonce = auth::generate_nonce_bytes();

    let client_first_payload = doc! {
        "r": Binary { subtype: BinarySubtype::Generic, bytes: nonce.clone().to_vec() },
        // `110` is ASCII for the character `n`, which is required by the spec to indicate that
        // channel binding is not supported.
        "p": 110i32,
    };
    let client_first_payload_bytes = client_first_payload.encode_to_vec()?;

    let sasl_start = SaslStart::new(
        source.into(),
        AuthMechanism::MongoDbAws,
        client_first_payload_bytes,
        server_api.cloned(),
    );
    let client_first = sasl_start.into_command()?;

    let server_first_response = conn.send_message(client_first).await?;

    let server_first = ServerFirst::parse(server_first_response.auth_response_body(MECH_NAME)?)?;
    server_first.validate(&nonce)?;

    let creds = get_aws_credentials(credential).await.map_err(|e| {
        Error::authentication_error(MECH_NAME, &format!("failed to get creds: {e}"))
    })?;
    let aws_credential = AwsCredential::from_sdk_creds(
        creds.access_key_id().to_string(),
        creds.secret_access_key().to_string(),
        creds.session_token().map(|s| s.to_string()),
        None,
    );

    // Find credentials using original implementation without AWS SDK
    // let aws_credential = {
    //     // Limit scope of this variable to avoid holding onto the lock for the duration of
    //     // authenticate_stream.
    //     let cached_credential = CACHED_CREDENTIAL.lock().await;
    //     match *cached_credential {
    //         Some(ref aws_credential) if !aws_credential.is_expired() => aws_credential.clone(),
    //         _ => {
    //             // From the spec: the driver MUST not place a lock on making a request.
    //             drop(cached_credential);
    //             let aws_credential = AwsCredential::get(credential, http_client).await?;
    //             if aws_credential.expiration.is_some() {
    //                 *CACHED_CREDENTIAL.lock().await = Some(aws_credential.clone());
    //             }
    //             aws_credential
    //         }
    //     }
    // };

    let date = Utc::now();

    let authorization_header = aws_credential.compute_authorization_header(
        date,
        &server_first.sts_host,
        &server_first.server_nonce,
    )?;

    let mut client_second_payload = doc! {
        "a": authorization_header,
        "d": date.format(AWS_LONG_DATE_FMT).to_string(),
    };

    if let Some(security_token) = aws_credential.session_token {
        client_second_payload.insert("t", security_token);
    }

    let client_second_payload_bytes = client_second_payload.encode_to_vec()?;

    let sasl_continue = SaslContinue::new(
        source.into(),
        server_first.conversation_id.clone(),
        client_second_payload_bytes,
        server_api.cloned(),
    );

    let client_second = sasl_continue.into_command();

    let server_second_response = conn.send_message(client_second).await?;
    let server_second = SaslResponse::parse(
        MECH_NAME,
        server_second_response.auth_response_body(MECH_NAME)?,
    )?;

    if server_second.conversation_id != server_first.conversation_id {
        return Err(Error::invalid_authentication_response(MECH_NAME));
    }

    if !server_second.done {
        return Err(Error::invalid_authentication_response(MECH_NAME));
    }

    Ok(())
}

// Find credentials using MongoDB URI or AWS SDK
pub async fn get_aws_credentials(credential: &Credential) -> Result<Credentials> {
    if let (Some(access_key), Some(secret_key)) = (&credential.username, &credential.password) {
        // Look for credentials in the MongoDB URI
        Ok(Credentials::new(
            access_key.clone(),
            secret_key.clone(),
            credential
                .mechanism_properties
                .as_ref()
                .and_then(|mp| mp.get_str("AWS_SESSION_TOKEN").ok())
                .map(str::to_owned),
            None,
            "MongoDB URI",
        ))
    } else {
        // If credentials are not provided in the URI, use the AWS SDK to load
        let creds = aws_config::load_defaults(BehaviorVersion::latest())
            .await
            .credentials_provider()
            .ok_or_else(|| {
                Error::authentication_error(MECH_NAME, "no credential provider configured")
            })?
            .provide_credentials()
            .await
            .map_err(|e| {
                Error::authentication_error(MECH_NAME, &format!("failed to get creds: {e}"))
            })?;
        Ok(creds)
    }
}

/// Contains the credentials for MONGODB-AWS authentication.
// RUST-1529 note: dead_code tag added to avoid unused warnings on expiration field
#[allow(dead_code)]
#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub(crate) struct AwsCredential {
    access_key_id: String,

    secret_access_key: String,

    #[serde(alias = "Token")]
    session_token: Option<String>,

    #[serde(
        default,
        deserialize_with = "serde_util::deserialize_datetime_option_from_double_or_string"
    )]
    expiration: Option<crate::bson::DateTime>,
}

// fn non_empty(s: Option<String>) -> Option<String> {
//     match s {
//         None => None,
//         Some(s) if s.is_empty() => None,
//         Some(s) => Some(s),
//     }
// }

impl AwsCredential {
    // /// Derives the credentials for an authentication attempt given the set of credentials the
    // user /// passed in.
    // pub(crate) async fn get(credential: &Credential, http_client: &HttpClient) -> Result<Self> {
    //     let access_key = credential
    //         .username
    //         .clone()
    //         .or_else(|| non_empty(std::env::var("AWS_ACCESS_KEY_ID").ok()));
    //     let secret_key = credential
    //         .password
    //         .clone()
    //         .or_else(|| non_empty(std::env::var("AWS_SECRET_ACCESS_KEY").ok()));
    //     let session_token = credential
    //         .mechanism_properties
    //         .as_ref()
    //         .and_then(|d| d.get_str("AWS_SESSION_TOKEN").ok())
    //         .map(|s| s.to_string())
    //         .or_else(|| non_empty(std::env::var("AWS_SESSION_TOKEN").ok()));

    //     let found_access_key = access_key.is_some();
    //     let found_secret_key = secret_key.is_some();

    //     // If we have an access key and secret key, we can continue with the credentials we've
    //     // found.
    //     if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
    //         return Ok(Self {
    //             access_key_id: access_key,
    //             secret_access_key: secret_key,
    //             session_token,
    //             expiration: None,
    //         });
    //     }

    //     if found_access_key || found_secret_key {
    //         return Err(Error::authentication_error(
    //             MECH_NAME,
    //             "cannot specify only one of access key and secret key; either both or neither \
    //              must be provided",
    //         ));
    //     }

    //     if session_token.is_some() {
    //         return Err(Error::authentication_error(
    //             MECH_NAME,
    //             "cannot specify session token without both access key and secret key",
    //         ));
    //     }

    //     if let (Ok(token_file), Ok(role_arn)) = (
    //         std::env::var("AWS_WEB_IDENTITY_TOKEN_FILE"),
    //         std::env::var("AWS_ROLE_ARN"),
    //     ) {
    //         return Self::get_from_assume_role_with_web_identity_request(
    //             token_file,
    //             role_arn,
    //             http_client,
    //         )
    //         .await;
    //     }

    //     if let Ok(relative_uri) = std::env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
    //         Self::get_from_ecs(relative_uri, http_client).await
    //     } else {
    //         Self::get_from_ec2(http_client).await
    //     }
    // }

    // Creates AwsCredential from keys.
    fn from_sdk_creds(
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        expiration: Option<crate::bson::DateTime>,
    ) -> Self {
        Self {
            access_key_id,
            secret_access_key,
            session_token,
            expiration,
        }
    }

    // async fn get_from_assume_role_with_web_identity_request(
    //     token_file: String,
    //     role_arn: String,
    //     http_client: &HttpClient,
    // ) -> Result<Self> {
    //     let mut file = File::open(&token_file).map_err(|_| {
    //         Error::authentication_error(MECH_NAME, "could not open identity token file")
    //     })?;
    //     let mut buffer = Vec::<u8>::new();
    //     file.read_to_end(&mut buffer).map_err(|_| {
    //         Error::authentication_error(MECH_NAME, "could not read identity token file")
    //     })?;
    //     let token = std::str::from_utf8(&buffer).map_err(|_| {
    //         Error::authentication_error(MECH_NAME, "could not read identity token file")
    //     })?;

    //     let session_name = std::env::var("AWS_ROLE_SESSION_NAME")
    //         .unwrap_or_else(|_| Alphanumeric.sample_string(&mut rand::thread_rng(), 10));

    //     let query = rawdoc! {
    //         "Action": "AssumeRoleWithWebIdentity",
    //         "RoleSessionName": session_name,
    //         "RoleArn": role_arn,
    //         "WebIdentityToken": token,
    //         "Version": "2011-06-15",
    //     };

    //     let response = http_client
    //         .get("https://sts.amazonaws.com/")
    //         .headers(&[("Accept", "application/json")])
    //         .query(query)
    //         .send::<Document>()
    //         .await
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))?;

    //     let credential = response
    //         .get_document("AssumeRoleWithWebIdentityResponse")
    //         .and_then(|d| d.get_document("AssumeRoleWithWebIdentityResult"))
    //         .and_then(|d| d.get_document("Credentials"))
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))?
    //         .to_owned();

    //     Ok(crate::bson_compat::deserialize_from_document(credential)?)
    // }

    // /// Obtains credentials from the ECS endpoint.
    // async fn get_from_ecs(relative_uri: String, http_client: &HttpClient) -> Result<Self> {
    //     // Use the local IP address that AWS uses for ECS agents.
    //     let uri = format!("http://{}/{}", AWS_ECS_IP, relative_uri);

    //     http_client
    //         .get(&uri)
    //         .send()
    //         .await
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))
    // }

    // /// Obtains temporary credentials for an EC2 instance to use for authentication.
    // async fn get_from_ec2(http_client: &HttpClient) -> Result<Self> {
    //     let temporary_token = http_client
    //         .put(format!("http://{}/latest/api/token", AWS_EC2_IP))
    //         .headers(&[("X-aws-ec2-metadata-token-ttl-seconds", "30")])
    //         .send_and_get_string()
    //         .await
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))?;

    //     let role_name_uri = format!(
    //         "http://{}/latest/meta-data/iam/security-credentials/",
    //         AWS_EC2_IP
    //     );

    //     let role_name = http_client
    //         .get(&role_name_uri)
    //         .headers(&[("X-aws-ec2-metadata-token", &temporary_token[..])])
    //         .send_and_get_string()
    //         .await
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))?;

    //     let credential_uri = format!("{}/{}", role_name_uri, role_name);

    //     http_client
    //         .get(&credential_uri)
    //         .headers(&[("X-aws-ec2-metadata-token", &temporary_token[..])])
    //         .send()
    //         .await
    //         .map_err(|_| Error::unknown_authentication_error(MECH_NAME))
    // }

    /// Computes the signed authorization header for the credentials to send to the server in a sasl
    /// payload.
    fn compute_authorization_header(
        &self,
        date: DateTime<Utc>,
        host: &str,
        server_nonce: &[u8],
    ) -> Result<String> {
        let date_str = date.format(AWS_LONG_DATE_FMT).to_string();

        // We need to include the security token header if the user provided a token. If not, we
        // just use the empty string.
        let token = self
            .session_token
            .as_ref()
            .map(|s| format!("x-amz-security-token:{}\n", s))
            .unwrap_or_default();

        // Similarly, we need to put "x-amz-security-token" into the list of signed headers if the
        // user provided a token. If not, we just use the empty string.
        let token_signed_header = if self.session_token.is_some() {
            "x-amz-security-token;"
        } else {
            ""
        };

        // Generate the list of signed headers (either with or without the security token header).
        #[rustfmt::skip]
        let signed_headers = format!(
            "\
              content-length;\
              content-type;\
              host;\
              x-amz-date;\
              {token_signed_header}\
              x-mongodb-gs2-cb-flag;\
              x-mongodb-server-nonce\
            ",
            token_signed_header = token_signed_header,
        );

        let body = "Action=GetCallerIdentity&Version=2011-06-15";
        let hashed_body = hex::encode(Sha256::digest(body.as_bytes()));

        let nonce = base64::encode(server_nonce);

        #[rustfmt::skip]
        let request = format!(
            "\
             POST\n\
             /\n\n\
             content-length:43\n\
             content-type:application/x-www-form-urlencoded\n\
             host:{host}\n\
             x-amz-date:{date}\n\
             {token}\
             x-mongodb-gs2-cb-flag:n\n\
             x-mongodb-server-nonce:{nonce}\n\n\
             {signed_headers}\n\
             {hashed_body}\
             ",
            host = host,
            date = date_str,
            token = token,
            nonce = nonce,
            signed_headers = signed_headers,
            hashed_body = hashed_body,
        );

        let hashed_request = hex::encode(Sha256::digest(request.as_bytes()));

        let small_date = date.format("%Y%m%d").to_string();

        let region = if host == "sts.amazonaws.com" {
            "us-east-1"
        } else {
            let parts: Vec<_> = host.split('.').collect();
            parts.get(1).copied().unwrap_or("us-east-1")
        };

        #[rustfmt::skip]
        let string_to_sign = format!(
            "\
             AWS4-HMAC-SHA256\n\
             {full_date}\n\
             {small_date}/{region}/sts/aws4_request\n\
             {hashed_request}\
            ",
            full_date = date_str,
            small_date = small_date,
            region = region,
            hashed_request = hashed_request,
        );

        let first_hmac_key = format!("AWS4{}", self.secret_access_key);
        let k_date =
            auth::mac::<Hmac<Sha256>>(first_hmac_key.as_ref(), small_date.as_ref(), MECH_NAME)?;
        let k_region = auth::mac::<Hmac<Sha256>>(k_date.as_ref(), region.as_ref(), MECH_NAME)?;
        let k_service = auth::mac::<Hmac<Sha256>>(k_region.as_ref(), b"sts", MECH_NAME)?;
        let k_signing = auth::mac::<Hmac<Sha256>>(k_service.as_ref(), b"aws4_request", MECH_NAME)?;

        let signature_bytes =
            auth::mac::<Hmac<Sha256>>(k_signing.as_ref(), string_to_sign.as_ref(), MECH_NAME)?;
        let signature = hex::encode(signature_bytes);

        #[rustfmt::skip]
        let auth_header = format!(
            "\
             AWS4-HMAC-SHA256 \
             Credential={access_key}/{small_date}/{region}/sts/aws4_request, \
             SignedHeaders={signed_headers}, \
             Signature={signature}\
            ",
            access_key = self.access_key_id,
            small_date = small_date,
            region = region,
            signed_headers = signed_headers,
            signature = signature
        );

        Ok(auth_header)
    }

    // #[cfg(feature = "in-use-encryption")]
    // pub(crate) fn access_key(&self) -> &str {
    //     &self.access_key_id
    // }

    // #[cfg(feature = "in-use-encryption")]
    // pub(crate) fn secret_key(&self) -> &str {
    //     &self.secret_access_key
    // }

    // #[cfg(feature = "in-use-encryption")]
    // pub(crate) fn session_token(&self) -> Option<&str> {
    //     self.session_token.as_deref()
    // }

    // RUST-1529 note: commented out is_expired method since it is not used in the current
    // implementation
    // fn is_expired(&self) -> bool {
    //     match self.expiration {
    //         Some(expiration) => {
    //             expiration.saturating_duration_since(crate::bson::DateTime::now())
    //                 < Duration::from_secs(5 * 60)
    //         }
    //         None => true,
    //     }
    // }
}

/// The response from the server to the `saslStart` command in a MONGODB-AWS authentication attempt.
struct ServerFirst {
    conversation_id: Bson,
    server_nonce: Vec<u8>,
    sts_host: String,
    done: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerFirstPayload {
    #[serde(rename = "s", with = "serde_bytes")]
    server_nonce: Vec<u8>,

    #[serde(rename = "h")]
    sts_host: String,
}

impl ServerFirst {
    /// Parses the response of the `saslStart` command.
    fn parse(response: Document) -> Result<Self> {
        let SaslResponse {
            conversation_id,
            payload,
            done,
        } = SaslResponse::parse(MECH_NAME, response)?;

        let ServerFirstPayload {
            server_nonce,
            sts_host,
        } = crate::bson_compat::deserialize_from_slice(payload.as_slice())
            .map_err(|_| Error::invalid_authentication_response(MECH_NAME))?;

        Ok(Self {
            conversation_id,
            server_nonce,
            sts_host,
            done,
        })
    }

    /// Ensures that the server sent a valid response to the `saslStart` command.
    fn validate(&self, nonce: &[u8]) -> Result<()> {
        if self.done {
            Err(Error::authentication_error(
                MECH_NAME,
                "handshake terminated early",
            ))
        } else if !self.server_nonce.starts_with(nonce) {
            Err(Error::authentication_error(MECH_NAME, "mismatched nonce"))
        } else if self.server_nonce.len() != 64 {
            Err(Error::authentication_error(
                MECH_NAME,
                "incorrect length server nonce",
            ))
        } else if self.sts_host.is_empty() {
            Err(Error::authentication_error(
                MECH_NAME,
                "sts host must be non-empty",
            ))
        } else if self.sts_host.len() > 255 {
            Err(Error::authentication_error(
                MECH_NAME,
                "sts host cannot be more than 255 bytes",
            ))
        } else if self.sts_host.split('.').any(|s| s.is_empty()) {
            Err(Error::authentication_error(
                MECH_NAME,
                "sts host cannot contain empty labels",
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::{AwsCredential, CACHED_CREDENTIAL};

    pub(crate) async fn cached_credential() -> Option<AwsCredential> {
        CACHED_CREDENTIAL.lock().await.clone()
    }

    pub(crate) async fn clear_cached_credential() {
        *CACHED_CREDENTIAL.lock().await = None;
    }

    pub(crate) async fn poison_cached_credential() {
        CACHED_CREDENTIAL
            .lock()
            .await
            .as_mut()
            .unwrap()
            .access_key_id = "bad".into();
    }

    pub(crate) async fn cached_access_key_id() -> String {
        cached_credential().await.unwrap().access_key_id
    }

    pub(crate) async fn cached_secret_access_key() -> String {
        cached_credential().await.unwrap().secret_access_key
    }

    pub(crate) async fn cached_session_token() -> Option<String> {
        cached_credential().await.unwrap().session_token
    }

    pub(crate) async fn cached_expiration() -> crate::bson::DateTime {
        cached_credential().await.unwrap().expiration.unwrap()
    }

    pub(crate) async fn set_cached_expiration(expiration: crate::bson::DateTime) {
        CACHED_CREDENTIAL.lock().await.as_mut().unwrap().expiration = Some(expiration);
    }
}
