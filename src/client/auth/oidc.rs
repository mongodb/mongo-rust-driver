use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

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
    cache: Arc<RwLock<Cache>>,
}

impl State {
    pub(crate) async fn get_refresh_token(&self) -> Option<String> {
        self.cache.read().await.refresh_token.clone()
    }

    // TODO RUST-1662: This function will actually be used.
    #[allow(dead_code)]
    pub(crate) async fn get_access_token(&self) -> Option<String> {
        self.cache.read().await.access_token.clone()
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct Callback {
    inner: Arc<CallbackInner>,
    kind: CallbackKind,
}

#[non_exhaustive]
#[derive(Clone, Copy)]
enum CallbackKind {
    Human,
    Machine,
}

// TODO RUST-1497: These will no longer be dead_code
#[allow(dead_code)]
impl Callback {
    fn new<F>(callback: F, kind: CallbackKind) -> Callback
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        Callback {
            inner: Arc::new(CallbackInner {
                f: Box::new(callback),
            }),
            kind,
        }
    }

    /// Create a new human token request callback.
    pub fn human<F>(callback: F) -> State
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        Self::create_state(callback, CallbackKind::Human)
    }

    /// Create a new machine token request callback.
    pub fn machine<F>(callback: F) -> State
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        Self::create_state(callback, CallbackKind::Machine)
    }

    fn create_state<F>(callback: F, kind: CallbackKind) -> State
    where
        F: Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        State {
            callback: Self::new(callback, kind),
            cache: Arc::new(RwLock::new(Cache::new())),
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
    refresh_token: Option<String>,
    access_token: Option<String>,
    token_gen_id: i32,
    last_call_time: Instant,
}

impl Clone for Cache {
    fn clone(&self) -> Self {
        Self {
            refresh_token: self.refresh_token.clone(),
            access_token: self.access_token.clone(),
            token_gen_id: self.token_gen_id,
            last_call_time: self.last_call_time,
        }
    }
}

impl Cache {
    fn new() -> Self {
        Self {
            refresh_token: None,
            access_token: None,
            token_gen_id: 0,
            last_call_time: Instant::now(),
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
#[non_exhaustive]
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
    pub expires: Option<Instant>,
    pub refresh_token: Option<String>,
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    // RUST-1662: Attempt speculative auth first, only works with a cache.
    // First handle speculative authentication. If that succeeds, we are done.

    let Callback { inner, kind } = credential
        .oidc_callback
        .as_ref()
        .ok_or_else(|| auth_error("no callbacks supplied"))?
        .callback
        .clone();
    match kind {
        CallbackKind::Machine => authenticate_machine(conn, credential, server_api, inner).await,
        CallbackKind::Human => authenticate_human(conn, credential, server_api, inner).await,
    }
}

async fn update_oidc_cache(
    credential: &Credential,
    response: &IdpServerResponse,
    token_gen_id: i32,
) {
    {
        let mut cache = credential
            .oidc_callback
            .as_ref()
            // unwrap() is safe here because authenticate_human is only called if oidc_callback is Some
            .unwrap()
            .cache
            .write()
            .await;
        cache.access_token = Some(response.access_token.clone());
        cache.refresh_token = response.refresh_token.clone();
        cache.last_call_time = Instant::now();
        cache.token_gen_id = token_gen_id;
    }
}

async fn authenticate_human(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    callback: Arc<CallbackInner>,
) -> Result<()> {
    // TODO RUST-1662: Use the Cached credential and add Cache invalidation
    // this differs from the machine flow in that we will also try the refresh token
    let source = credential.source.as_deref().unwrap_or("$external");
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

    // Even though most caching will be handled in RUST-1662, the refresh token only exists in the
    // cache, so we need to access the cache to get it
    let refresh_token = credential.oidc_callback.as_ref()
        // this unwrap is safe because we are in the authenticate_human function which only gets called if oidc_callback is Some
        .unwrap().get_refresh_token().await;

    let idp_response = {
        let server_info: IdpServerInfo =
            bson::from_slice(&response.payload).map_err(|_| invalid_auth_response())?;
        const CALLBACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
        let cb_context = CallbackContext {
            timeout_seconds: Some(Instant::now() + CALLBACK_TIMEOUT),
            version: 1,
            refresh_token,
            idp_info: Some(server_info),
        };
        (callback.f)(cb_context).await?
    };

    // we'll go ahead and update the cache, also,
    // TODO RUST 1662: Modify this comment to just say we are updating the cache
    update_oidc_cache(credential, &idp_response, 1).await;

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

async fn authenticate_machine(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    callback: Arc<CallbackInner>,
) -> Result<()> {
    // TODO RUST-1662: Use the Cached credential and add Cache invalidation
    let source = credential.source.as_deref().unwrap_or("$external");
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

    // we'll go ahead and update the cache, also,
    // TODO RUST 1662: Modify this comment to just say we are updating the cache
    update_oidc_cache(credential, &idp_response, 1).await;

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
