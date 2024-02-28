use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bson::rawdoc;
use serde::Deserialize;
use typed_builder::TypedBuilder;

use crate::{
    client::{
        auth::{
            sasl::{SaslResponse, SaslStart},
            AuthMechanism,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
    BoxFuture,
};

use super::{sasl::SaslContinue, Credential, MONGODB_OIDC_STR};

/// The user-supplied callbacks for OIDC authentication.
#[derive(Clone)]
pub struct State {
    callback: Callback,
    cache: Cache,
}

#[derive(Clone)]
pub(crate) enum Callback {
    Machine(Arc<CallbackInner>),
    Human(Arc<CallbackInner>),
}

impl Callback {
    /// Create a new instance with a human token request callback.
    pub fn human<F>(callback: F) -> State
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        State {
            callback: Callback::Human(Arc::new(CallbackInner {
                f: Box::new(callback),
            })),
            cache: Cache::default(),
        }
    }

    /// Create a new instance with a machine token request callback.
    pub fn machine<F>(callback: F) -> State
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        State {
            callback: Callback::Machine(Arc::new(CallbackInner {
                f: Box::new(callback),
            })),
            cache: Cache::default(),
        }
    }
}

impl std::fmt::Debug for Callback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Callback").finish()
    }
}

pub struct CallbackInner {
    f: Box<dyn Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>> + Send + Sync>,
}

#[derive(Debug)]
pub struct Cache {
    username: Option<String>,
    properties: Properties,
    refresh_token: Option<String>,
    access_token: Option<String>,
    idp_info: Option<IdpServerInfo>,
    token_gen_id: i32,
    last_call_time: Instant,
    lock: tokio::sync::Mutex<()>,
}

impl Clone for Cache {
    fn clone(&self) -> Self {
        Self {
            username: self.username.clone(),
            properties: self.properties.clone(),
            refresh_token: self.refresh_token.clone(),
            access_token: self.access_token.clone(),
            idp_info: self.idp_info.clone(),
            token_gen_id: self.token_gen_id,
            last_call_time: self.last_call_time,
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self {
            username: None,
            properties: Properties {
                provider_name: None,
                allowed_hosts: Vec::new(),
            },
            refresh_token: None,
            access_token: None,
            idp_info: None,
            token_gen_id: 0,
            last_call_time: Instant::now(),
            lock: tokio::sync::Mutex::new(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IdpServerInfo {
    pub issuer: String,
    pub client_id: String,
    pub request_scopes: Option<Vec<String>>,
}

#[derive(Debug)]
pub struct CallbackContext {
    pub timeout_seconds: Option<Instant>,
    pub version: i32,
    pub refresh_token: Option<String>,
    pub idp_info: Option<IdpServerInfo>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct IdpServerResponse {
    pub access_token: String,
    pub expires_in_seconds: Option<Instant>,
    pub refresh_token: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Properties {
    pub provider_name: Option<String>,
    pub allowed_hosts: Vec<String>,
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    // TODO RUST-1662: Use the Cached credential and add Cache invalidation
    let source = credential.source.as_deref().unwrap_or("$external");
    let callback = if let Callback::Machine(callback) = credential
        .oidc_callback
        .as_ref()
        .ok_or_else(|| auth_error("no callbacks supplied"))?
        .callback
        .clone()
    {
        callback
    } else {
        todo!()
    };

    let mut start_doc = rawdoc! {};
    if let Some(username) = credential.username.as_deref() {
        start_doc.append("n", username);
    }
    let sasl_start = SaslStart::new(
        source.to_string(),
        AuthMechanism::MongoDbOidc,
        start_doc.into_bytes(),
        server_api.cloned(),
    )
    .into_command();
    let response = send_sasl_command(conn, sasl_start).await?;
    if response.done {
        return Err(invalid_auth_response());
    }
    let idp_response = {
        let server_info: IdpServerInfo =
            bson::from_slice(&response.payload).map_err(|_| invalid_auth_response())?;
        const CALLBACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
        let cb_context = CallbackContext {
            timeout_seconds: Some(Instant::now() + CALLBACK_TIMEOUT),
            version: 1,
            refresh_token: None,
            idp_info: Some(server_info),
        };
        (callback.f)(cb_context).await?
    };

    let sasl_continue = SaslContinue::new(
        source.to_string(),
        response.conversation_id,
        rawdoc! { "jwt": idp_response.access_token }.into_bytes(),
        server_api.cloned(),
    )
    .into_command();
    let response = send_sasl_command(conn, sasl_continue).await?;
    if !response.done {
        return Err(invalid_auth_response());
    }

    Ok(())
}

fn auth_error(s: impl AsRef<str>) -> Error {
    Error::authentication_error(MONGODB_OIDC_STR, s.as_ref())
}

fn invalid_auth_response() -> Error {
    Error::invalid_authentication_response(MONGODB_OIDC_STR)
}

async fn send_sasl_command(
    conn: &mut Connection,
    command: crate::cmap::Command,
) -> Result<SaslResponse> {
    let response = conn.send_command(command, None).await?;
    SaslResponse::parse(
        MONGODB_OIDC_STR,
        response.auth_response_body(MONGODB_OIDC_STR)?,
    )
}
