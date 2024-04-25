use serde::Deserialize;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
#[cfg(feature = "azure-oidc")]
use tokio::sync::MutexGuard;
use typed_builder::TypedBuilder;

#[cfg(feature = "azure-oidc")]
use crate::client::auth::{
    AZURE_ENVIRONMENT_VALUE_STR, ENVIRONMENT_PROP_STR, TOKEN_RESOURCE_PROP_STR,
};
use crate::{
    client::{
        auth::{
            sasl::{SaslResponse, SaslStart},
            AuthMechanism, ALLOWED_HOSTS_PROP_STR,
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

#[derive(Clone)]
pub struct State {
    inner: Arc<Mutex<Option<StateInner>>>,
    is_user_provided: bool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
            is_user_provided: false,
        }
    }
}

impl State {
    pub(crate) fn is_user_provided(&self) -> bool {
        self.is_user_provided
    }

    #[cfg(not(feature = "azure-oidc"))]
    #[cfg(test)]
    pub(crate) async fn set_access_token(&self, access_token: Option<String>) {
        self.inner.lock().await.as_mut().unwrap().cache.access_token = access_token;
    }

    #[cfg(not(feature = "azure-oidc"))]
    #[cfg(test)]
    pub(crate) async fn set_refresh_token(&self, refresh_token: Option<String>) {
        self.inner
            .lock()
            .await
            .as_mut()
            .unwrap()
            .cache
            .refresh_token = refresh_token;
    }
}

/// The user-supplied callbacks for OIDC authentication.
#[derive(Clone, Debug)]
pub struct StateInner {
    callback: Callback,
    cache: Cache,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct Callback {
    inner: Arc<CallbackInner>,
    kind: CallbackKind,
}

#[non_exhaustive]
#[derive(Clone, Copy, Debug)]
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
            inner: Arc::new(Mutex::new(Some(StateInner {
                callback: Self::new(callback, kind),
                cache: Cache::new(),
            }))),
            is_user_provided: true,
        }
    }

    /// Create azure callback.
    #[cfg(feature = "azure-oidc")]
    fn azure_callback(client_id: Option<&str>, resource: &str) -> StateInner {
        use futures_util::FutureExt;
        let resource = resource.to_string();
        let client_id = client_id.map(|s| s.to_string());
        let mut url = format!(
            "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource={}",
            resource
        );
        if let Some(ref client_id) = client_id {
            url.push_str(&format!("&client_id={}", client_id));
        }
        StateInner {
            callback: Self::new(
                move |_| {
                    let url = url.clone();
                    async move {
                        let url = url.clone();
                        let response = crate::runtime::HttpClient::default()
                            .get(&url)
                            .headers(&[("Metadata", "true"), ("Accept", "application/json")])
                            .send::<Document>()
                            .await
                            .map_err(|e| {
                                Error::authentication_error(
                                    MONGODB_OIDC_STR,
                                    &format!("Failed to get access token from Azure IDMS: {}", e),
                                )
                            });
                        let response = response?;
                        let access_token = response
                            .get_str("access_token")
                            .map_err(|e| {
                                Error::authentication_error(
                                    MONGODB_OIDC_STR,
                                    &format!("Failed to get access token from Azure IDMS: {}", e),
                                )
                            })?
                            .to_string();
                        let expires_in = response
                            .get_str("expires_in")
                            .map_err(|e| {
                                Error::authentication_error(
                                    MONGODB_OIDC_STR,
                                    &format!("Failed to get expires_in from Azure IDMS: {}", e),
                                )
                            })?
                            .parse::<u64>()
                            .map_err(|e| {
                                Error::authentication_error(
                                    MONGODB_OIDC_STR,
                                    &format!(
                                        "Failed to parse expires_in from Azure IDMS as u64: {}",
                                        e
                                    ),
                                )
                            })?;
                        let expires = Some(Instant::now() + Duration::from_secs(expires_in as u64));
                        Ok(IdpServerResponse {
                            access_token,
                            expires,
                            refresh_token: None,
                        })
                    }
                    .boxed()
                },
                CallbackKind::Machine,
            ),
            cache: Cache::automatic_callback_cache(),
        }
    }
}

use std::fmt::Debug;
impl std::fmt::Debug for Callback {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(format!("Callback: {:?}", self.kind).as_str())
            .finish()
    }
}

pub struct CallbackInner {
    f: Box<dyn Fn(CallbackContext) -> BoxFuture<'static, Result<IdpServerResponse>> + Send + Sync>,
}

#[derive(Debug, Clone)]
pub struct Cache {
    idp_server_info: Option<IdpServerInfo>,
    refresh_token: Option<String>,
    access_token: Option<String>,
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

    // The cache for automatic providers should have Some(idp_server_info) so that
    // we do not attempt a two step authorization flow.
    #[cfg(feature = "azure-oidc")]
    fn automatic_callback_cache() -> Self {
        Self {
            idp_server_info: Some(IdpServerInfo {
                issuer: "".to_string(),
                client_id: None,
                request_scopes: None,
            }),
            refresh_token: None,
            access_token: None,
            token_gen_id: 0,
            last_call_time: Instant::now(),
        }
    }

    async fn update(
        &mut self,
        response: &IdpServerResponse,
        idp_server_info: Option<IdpServerInfo>,
    ) {
        if idp_server_info.is_some() {
            self.idp_server_info = idp_server_info;
        }
        self.access_token = Some(response.access_token.clone());
        self.refresh_token = response.refresh_token.clone();
        self.last_call_time = Instant::now();
        self.token_gen_id += 1;
    }

    async fn propagate_token_gen_id(&mut self, conn: &Connection) {
        let mut token_gen_id = conn.oidc_token_gen_id.lock().await;
        if *token_gen_id < self.token_gen_id {
            *token_gen_id = self.token_gen_id;
        }
    }

    async fn invalidate(&mut self, conn: &Connection, force: bool) {
        let mut token_gen_id = conn.oidc_token_gen_id.lock().await;
        // It should be impossible for token_gen_id to be > cache.token_gen_id, but we check just in
        // case
        if force || *token_gen_id >= self.token_gen_id {
            self.access_token = None;
            *token_gen_id = 0;
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IdpServerInfo {
    pub issuer: String,
    pub client_id: Option<String>,
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
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct IdpServerResponse {
    #[builder(!default)]
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
    credential.oidc_callback.inner.lock().await.as_ref()?;
    if let Some(ref access_token) = credential
        .oidc_callback
        .inner
        .lock()
        .await
        .as_ref()
        .unwrap()
        .cache
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
    credential
        .oidc_callback
        .inner
        .lock()
        .await
        .as_mut()
        .unwrap()
        .cache
        .invalidate(conn, true)
        .await;
    authenticate_stream(conn, credential, server_api, None).await
}

#[cfg(feature = "azure-oidc")]
async fn setup_automatic_providers(
    credential: &Credential,
    guard: &mut MutexGuard<'_, Option<StateInner>>,
) {
    // If there is already a callback, there is no need to set up an automatic provider
    // this could happen in the case of a reauthentication, or if the user has already set up
    // a callback. A situation where the user has set up a callback and an automatic provider
    // would already have caused an InvalidArgument error in `validate_credential`.
    if guard.is_some() {
        return;
    }
    if let Some(ref p) = credential.mechanism_properties {
        let environment = p.get_str(ENVIRONMENT_PROP_STR).unwrap_or("");
        let client_id = credential.username.as_deref();
        let resource = p.get_str(TOKEN_RESOURCE_PROP_STR).unwrap_or("");
        match environment {
            AZURE_ENVIRONMENT_VALUE_STR => {
                **guard = Some(Callback::azure_callback(client_id, resource))
            }
            _ => {}
        }
    }
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
    let mut guard = credential.oidc_callback.inner.lock().await;

    #[cfg(feature = "azure-oidc")]
    setup_automatic_providers(credential, &mut guard).await;
    let StateInner {
        cache,
        callback: Callback { inner, kind },
    } = &mut guard
        .as_mut()
        .ok_or_else(|| auth_error("no callbacks supplied"))?;

    cache.propagate_token_gen_id(conn).await;

    if server_first.into().is_some() {
        // speculative authentication succeeded, no need to authenticate again
        // update the Connection gen_id to be that of the cred_cache
        cache.propagate_token_gen_id(conn).await;
        return Ok(());
    }
    let source = credential.source.as_deref().unwrap_or("$external");

    match kind {
        CallbackKind::Machine => {
            authenticate_machine(source, conn, credential, cache, server_api, inner.clone()).await
        }
        CallbackKind::Human => {
            authenticate_human(source, conn, credential, cache, server_api, inner.clone()).await
        }
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
    cred_cache: &mut Cache,
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
            let server_info = cred_cache.idp_server_info.clone();
            cred_cache.update(&idp_response, server_info).await;
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
    cred_cache.update(&idp_response, Some(server_info)).await;

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
        mechanism_properties.and_then(|p| p.get_array(ALLOWED_HOSTS_PROP_STR).ok())
    {
        return allowed_hosts
            .iter()
            .map(|host| {
                host.as_str().ok_or_else(|| {
                    auth_error(format!(
                        "`{}` must contain only strings",
                        ALLOWED_HOSTS_PROP_STR
                    ))
                })
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
    cred_cache: &mut Cache,
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
        cred_cache.invalidate(conn, false).await;
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
                cred_cache.update(&idp_response, None).await;
                return Ok(());
            }
            // It should really not be possible for this to occur, we would get an error, if the
            // response is not done. Just in case, we will fall through to two_step to try one
            // more time.
        } else {
            // since this is an error, we will go ahead and invalidate the caches so we do not
            // try to use them again and waste time. We should fall through so that we can
            // do the shared flow from the beginning
            cred_cache.invalidate(conn, false).await;
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
    cred_cache: &mut Cache,
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
        cred_cache.invalidate(conn, false).await;
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
