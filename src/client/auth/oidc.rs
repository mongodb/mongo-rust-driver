use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, MutexGuard};
use typed_builder::TypedBuilder;

use crate::{
    client::{
        auth::{
            sasl::{SaslResponse, SaslStart},
            AuthMechanism,
        },
        options::{ServerAddress, ServerApi},
    },
    cmap::{Command, Connection},
    error::{Error, Result},
    BoxFuture,
};
use bson::{doc, rawdoc, spec::BinarySubtype, Binary, Document};

use super::{sasl::SaslContinue, Credential, MONGODB_OIDC_STR};

const HUMAN_CALLBACK_TIMEOUT: Duration = Duration::from_secs(5 * 60);
const MACHINE_CALLBACK_TIMEOUT: Duration = Duration::from_secs(60);
const MACHINE_INVALIDATE_SLEEP_TIMEOUT: Duration = Duration::from_millis(100);
const API_VERSION: u32 = 1;
const DEFAULT_ALLOWED_HOSTS: &[&str] = &[
    "*.mongodb.net",
    "*.mongodb-qa.net",
    "*.mongodb-dev.net",
    "*.mongodbgov.net",
    "localhost",
    "127.0.0.1",
    "::1",
];

/// The user-supplied callbacks for OIDC authentication.
#[derive(Clone)]
pub struct State {
    callback: Callback,
    // pub(crate) for spec tests
    pub(crate) cache: Arc<Mutex<Cache>>,
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
            cache: Arc::new(Mutex::new(Cache::new())),
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

#[derive(Debug, Clone)]
pub struct Cache {
    idp_server_info: Option<IdpServerInfo>,
    // pub(crate) for spec tests
    pub(crate) refresh_token: Option<String>,
    // pub(crate) for spec tests
    pub(crate) access_token: Option<String>,
    token_gen_id: u32,
    last_call_time: Instant,
}

impl Cache {
    fn new() -> Self {
        Self {
            idp_server_info: None,
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
    pub version: u32,
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

fn make_spec_auth_command(
    source: String,
    payload: Vec<u8>,
    server_api: Option<&ServerApi>,
) -> Command {
    let body = doc! {
        "saslStart": 1,
        "mechanism": MONGODB_OIDC_STR,
        "payload": Binary { subtype: BinarySubtype::Generic, bytes: payload },
        "db": "$external",
    };

    let mut command = Command::new("saslStart", source, body);
    if let Some(server_api) = server_api {
        command.set_server_api(server_api);
    }
    command
}

pub(crate) async fn build_speculative_client_first(credential: &Credential) -> Option<Command> {
    self::build_client_first(credential, None).await
}

/// Constructs the first client message in the OIDC handshake for speculative authentication
pub(crate) async fn build_client_first(
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Option<Command> {
    credential.oidc_callback.as_ref()?;
    if let Some(ref access_token) = credential
        .oidc_callback
        .as_ref()
        .unwrap()
        .cache
        .lock()
        .await
        .access_token
    {
        let start_doc = rawdoc! {
            "jwt": access_token.clone()
        };
        let source = credential
            .source
            .clone()
            .unwrap_or_else(|| "$external".to_string());
        return Some(make_spec_auth_command(
            source,
            start_doc.as_bytes().to_vec(),
            server_api,
        ));
    }
    None
}

pub(crate) async fn reauthenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    invalidate_caches(
        conn,
        &mut credential
            .oidc_callback
            .as_ref()
            .unwrap()
            .cache
            .lock()
            .await,
        true,
    )
    .await;
    authenticate_stream(conn, credential, server_api, None).await
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    server_first: impl Into<Option<Document>>,
) -> Result<()> {
    // We need to hold the lock for the entire function so that multiple callbacks
    // are not called during an authentication race, and so that token_gen_id on the Connection
    // always matches that in the Credential Cache.
    let mut cred_cache = credential
        .oidc_callback
        .as_ref()
        .ok_or_else(|| auth_error("no callbacks supplied"))?
        .cache
        .lock()
        .await;

    propagate_token_gen_id(conn, &cred_cache).await;

    if server_first.into().is_some() {
        // speculative authentication succeeded, no need to authenticate again
        // update the Connection gen_id to be that of the cred_cache
        propagate_token_gen_id(conn, &cred_cache).await;
        return Ok(());
    }
    let source = credential.source.as_deref().unwrap_or("$external");

    let Callback { inner, kind } = credential
        .oidc_callback
        .as_ref()
        .ok_or_else(|| auth_error("no callbacks supplied"))?
        .callback
        .clone();
    match kind {
        CallbackKind::Machine => {
            authenticate_machine(source, conn, credential, &mut cred_cache, server_api, inner).await
        }
        CallbackKind::Human => {
            authenticate_human(source, conn, credential, &mut cred_cache, server_api, inner).await
        }
    }
}

async fn update_cred_cache(
    cred_cache: &mut MutexGuard<'_, Cache>,
    response: &IdpServerResponse,
    idp_server_info: Option<IdpServerInfo>,
) {
    if idp_server_info.is_some() {
        cred_cache.idp_server_info = idp_server_info;
    }
    cred_cache.access_token = Some(response.access_token.clone());
    cred_cache.refresh_token = response.refresh_token.clone();
    cred_cache.last_call_time = Instant::now();
    cred_cache.token_gen_id += 1;
}

async fn propagate_token_gen_id(conn: &Connection, cred_cache: &MutexGuard<'_, Cache>) {
    let mut token_gen_id = conn.oidc_token_gen_id.lock().await;
    if *token_gen_id < cred_cache.token_gen_id {
        *token_gen_id = cred_cache.token_gen_id;
    }
}

async fn invalidate_caches(conn: &Connection, cred_cache: &mut MutexGuard<'_, Cache>, force: bool) {
    let mut token_gen_id = conn.oidc_token_gen_id.lock().await;
    // It should be impossible for token_gen_id to be > cache.token_gen_id, but we check just in
    // case
    if force || *token_gen_id >= cred_cache.token_gen_id {
        cred_cache.access_token = None;
        *token_gen_id = 0;
    }
}

// send_sasl_start_command creates and sends a sasl_start command handling either
// one step or two step sasl based on whether or not the access token is Some.
async fn send_sasl_start_command(
    source: &str,
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    access_token: Option<String>,
) -> Result<SaslResponse> {
    let mut start_doc = rawdoc! {};
    if let Some(access_token) = access_token {
        start_doc.append("jwt", access_token);
    } else if let Some(username) = credential.username.as_deref() {
        start_doc.append("n", username);
    }
    let sasl_start = SaslStart::new(
        source.to_string(),
        AuthMechanism::MongoDbOidc,
        start_doc.into_bytes(),
        server_api.cloned(),
    )
    .into_command();
    send_sasl_command(conn, sasl_start).await
}

// do_shared_flow is the shared flow for both human and machine
async fn do_shared_flow(
    source: &str,
    conn: &mut Connection,
    cred_cache: &mut MutexGuard<'_, Cache>,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    callback: Arc<CallbackInner>,
    timeout: Duration,
) -> Result<()> {
    // If the idpinfo is cached, we use that instead of doing two_step. It seems the spec does not
    // allow idpinfo to change on invalidations.
    if cred_cache.idp_server_info.is_some() {
        let idp_response = {
            let cb_context = CallbackContext {
                timeout_seconds: Some(Instant::now() + HUMAN_CALLBACK_TIMEOUT),
                version: API_VERSION,
                refresh_token: None,
                idp_info: cred_cache.idp_server_info.clone(),
            };
            (callback.f)(cb_context).await?
        };
        let response = send_sasl_start_command(
            source,
            conn,
            credential,
            server_api,
            Some(idp_response.access_token.clone()),
        )
        .await?;
        if response.done {
            update_cred_cache(
                cred_cache,
                &idp_response,
                cred_cache.idp_server_info.clone(),
            )
            .await;
            return Ok(());
        }
        return Err(invalid_auth_response());
    }

    // Here we do not have the idpinfo, so we need to do the two step flow.
    let response = send_sasl_start_command(source, conn, credential, server_api, None).await?;
    if response.done {
        return Err(invalid_auth_response());
    }

    let server_info: IdpServerInfo =
        bson::from_slice(&response.payload).map_err(|_| invalid_auth_response())?;
    let idp_response = {
        let cb_context = CallbackContext {
            timeout_seconds: Some(Instant::now() + timeout),
            version: API_VERSION,
            refresh_token: None,
            idp_info: Some(server_info.clone()),
        };
        (callback.f)(cb_context).await?
    };

    // Update the credential and connection caches with the access token and the credential cache
    // with the refresh token and token_gen_id
    update_cred_cache(cred_cache, &idp_response, Some(server_info)).await;

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

fn get_allowed_hosts(mechanism_properties: Option<&Document>) -> Result<Vec<&str>> {
    if mechanism_properties.is_none() {
        return Ok(Vec::from(DEFAULT_ALLOWED_HOSTS));
    }
    if let Some(allowed_hosts) =
        mechanism_properties.and_then(|p| p.get_array("ALLOWED_HOSTS").ok())
    {
        return allowed_hosts
            .iter()
            .map(|host| {
                host.as_str()
                    .ok_or_else(|| auth_error("ALLOWED_HOSTS must contain only strings"))
            })
            .collect::<Result<Vec<_>>>();
    }
    Ok(Vec::from(DEFAULT_ALLOWED_HOSTS))
}

fn validate_address_with_allowed_hosts(
    mechanism_properties: Option<&Document>,
    address: &ServerAddress,
) -> Result<()> {
    let hostname = if let ServerAddress::Tcp { host, .. } = address {
        host.as_str()
    } else {
        return Err(auth_error("OIDC human flow only supports TCP addresses"));
    };
    for pattern in get_allowed_hosts(mechanism_properties)? {
        if pattern == hostname {
            return Ok(());
        }
        if pattern.starts_with("*.") && hostname.ends_with(&pattern[1..]) {
            return Ok(());
        }
    }
    Err(auth_error(
        "The Connection address is not in the allowed list of hosts",
    ))
}

async fn authenticate_human(
    source: &str,
    conn: &mut Connection,
    credential: &Credential,
    cred_cache: &mut MutexGuard<'_, Cache>,
    server_api: Option<&ServerApi>,
    callback: Arc<CallbackInner>,
) -> Result<()> {
    validate_address_with_allowed_hosts(credential.mechanism_properties.as_ref(), &conn.address)?;

    // We need to hold the lock for the entire function so that multiple callbacks
    // are not called during an authentication race.

    // If the access token is in the cache, we can use it to send the sasl start command and avoid
    // the callback and sasl_continue
    if let Some(ref access_token) = cred_cache.access_token {
        let response = send_sasl_start_command(
            source,
            conn,
            credential,
            server_api,
            Some(access_token.clone()),
        )
        .await;
        if let Ok(response) = response {
            if response.done {
                return Ok(());
            }
        }
        invalidate_caches(conn, cred_cache, false).await;
    }

    // If the cache has a refresh token, we can avoid asking for the server info.
    if let (refresh_token @ Some(_), idp_info) = (
        cred_cache.refresh_token.clone(),
        cred_cache.idp_server_info.clone(),
    ) {
        let idp_response = {
            let cb_context = CallbackContext {
                timeout_seconds: Some(Instant::now() + HUMAN_CALLBACK_TIMEOUT),
                version: API_VERSION,
                refresh_token,
                idp_info,
            };
            (callback.f)(cb_context).await?
        };

        let access_token = idp_response.access_token.clone();
        let response =
            send_sasl_start_command(source, conn, credential, server_api, Some(access_token)).await;
        if let Ok(response) = response {
            if response.done {
                // Update the credential and connection caches with the access token and the
                // credential cache with the refresh token and token_gen_id
                update_cred_cache(cred_cache, &idp_response, None).await;
                return Ok(());
            }
            // It should really not be possible for this to occur, we would get an error, if the
            // response is not done. Just in case, we will fall through to two_step to try one
            // more time.
        } else {
            // since this is an error, we will go ahead and invalidate the caches so we do not
            // try to use them again and waste time. We should fall through so that we can
            // do the shared flow from the beginning
            invalidate_caches(conn, cred_cache, false).await;
        }
    }

    do_shared_flow(
        source,
        conn,
        cred_cache,
        credential,
        server_api,
        callback,
        HUMAN_CALLBACK_TIMEOUT,
    )
    .await
}

async fn authenticate_machine(
    source: &str,
    conn: &mut Connection,
    credential: &Credential,
    cred_cache: &mut MutexGuard<'_, Cache>,
    server_api: Option<&ServerApi>,
    callback: Arc<CallbackInner>,
) -> Result<()> {
    // If the access token is in the cache, we can use it to send the sasl start command and avoid
    // the callback and sasl_continue
    if let Some(ref access_token) = cred_cache.access_token {
        let response = send_sasl_start_command(
            source,
            conn,
            credential,
            server_api,
            Some(access_token.clone()),
        )
        .await;
        if let Ok(response) = response {
            if response.done {
                return Ok(());
            }
        }
        invalidate_caches(conn, cred_cache, false).await;
        tokio::time::sleep(MACHINE_INVALIDATE_SLEEP_TIMEOUT).await;
    }

    do_shared_flow(
        source,
        conn,
        cred_cache,
        credential,
        server_api,
        callback,
        MACHINE_CALLBACK_TIMEOUT,
    )
    .await
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
