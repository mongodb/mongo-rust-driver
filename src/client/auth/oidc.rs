use std::{time::Instant, sync::Arc};

use bson::rawdoc;
use futures_core::future::BoxFuture;
use serde::Deserialize;

use crate::{cmap::Connection, client::{options::ServerApi, auth::{sasl::{SaslStart, SaslResponse}, AuthMechanism}}, error::{Result, Error}};

use super::{Credential, MONGODB_OIDC_STR, sasl::SaslContinue};

/// dbg!
#[derive(Clone)]
pub struct Callbacks {
    inner: Arc<CallbacksInner>,
}

impl Callbacks {
    /// dbg!
    pub fn new<F>(on_request: F) -> Self
        where F: Fn(&IdpServerInfo, &RequestParameters) -> BoxFuture<'static, IdpServerResponse> + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(CallbacksInner { on_request: Box::new(on_request) })
        }
    }
}

struct CallbacksInner {
    on_request: Box<dyn Fn(&IdpServerInfo, &RequestParameters) -> BoxFuture<'static, IdpServerResponse> + Send + Sync>,
    //on_refresh: Option<Box<dyn Fn(&IdpServerInfo) -> IdpServerResponse + Send + Sync>>,
}

#[derive(Deserialize)]
#[non_exhaustive]
pub struct IdpServerInfo {
    pub issuer: String,
    pub client_id: String,
    pub request_scopes: Vec<String>,
}

#[non_exhaustive]
pub struct IdpServerResponse {
    pub access_token: String,
    pub expires: Instant,
    pub refresh_token: String,
}

#[non_exhaustive]
pub struct RequestParameters {
    pub timeout: Instant,
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    let source = credential.source.as_deref().unwrap_or("$external");
    let username = credential
        .username
        .as_deref()
        .ok_or_else(|| auth_error("no username supplied"))?;

    let payload = rawdoc! {
        "n": username,
    }.into_bytes();
    let sasl_start = SaslStart::new(
        source.to_string(),
        AuthMechanism::MongoDbOidc,
        payload,
        server_api.cloned(),
    )
    .into_command();

    let response = send_sasl_command(conn, sasl_start).await?;
    if response.done {
        return Err(invalid_auth_response());
    }
    let server_info: IdpServerInfo = bson::from_slice(&response.payload)
        .map_err(|_| invalid_auth_response())?;

    let idp_response: IdpServerResponse = todo!();

    let payload = rawdoc! {
        "jwt": idp_response.access_token,
    }.into_bytes();
    let sasl_continue = SaslContinue::new(
        source.to_string(),
        response.conversation_id,
        payload,
        server_api.cloned(),
    ).into_command();

    let response = send_sasl_command(conn, sasl_start).await?;
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

async fn send_sasl_command(conn: &mut Connection, command: crate::cmap::Command) -> Result<SaslResponse> {
    let response = conn.send_command(command, None).await?;
    SaslResponse::parse(MONGODB_OIDC_STR, response.auth_response_body(MONGODB_OIDC_STR)?)
}