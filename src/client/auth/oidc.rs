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
    error::{Error, Result}, BoxFuture,
};

use super::{sasl::SaslContinue, Credential, MONGODB_OIDC_STR};

/// The user-supplied callbacks for OIDC authentication.
#[derive(Clone)]
pub struct Callbacks {
    inner: Arc<CallbacksInner>,
}

impl Callbacks {
    /// Create a new instance with a token request callback.
    pub fn new<F>(on_request: F) -> Self
    where
        F: Fn(IdpServerInfo, RequestParameters) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync
            + 'static,
    {
        Self {
            inner: Arc::new(CallbacksInner {
                on_request: Box::new(on_request),
            }),
        }
    }
}

impl std::fmt::Debug for Callbacks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Callbacks").finish()
    }
}

struct CallbacksInner {
    on_request: Box<
        dyn Fn(IdpServerInfo, RequestParameters) -> BoxFuture<'static, Result<IdpServerResponse>>
            + Send
            + Sync,
    >,
    // on_refresh: Option<Box<dyn Fn(&IdpServerInfo) -> IdpServerResponse + Send + Sync>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct IdpServerInfo {
    pub issuer: String,
    pub client_id: String,
    pub request_scopes: Option<Vec<String>>,
}

#[derive(TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct IdpServerResponse {
    pub access_token: String,
    pub expires: Option<Instant>,
    pub refresh_token: Option<String>,
}

#[derive(Debug)]
#[non_exhaustive]
pub struct RequestParameters {
    pub deadline: Instant,
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    let source = credential.source.as_deref().unwrap_or("$external");
    let callbacks = credential
        .oidc_callbacks
        .as_ref()
        .ok_or_else(|| auth_error("no callbacks supplied"))?
        .clone();

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
        let cb_params = RequestParameters {
            deadline: Instant::now() + CALLBACK_TIMEOUT,
        };
        (callbacks.inner.on_request)(server_info, cb_params).await?
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
