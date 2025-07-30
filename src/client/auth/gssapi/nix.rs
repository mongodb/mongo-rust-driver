use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};

use crate::{
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            GSSAPI_STR,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
};

pub(super) async fn authenticate_stream(
    conn: &mut Connection,
    server_api: Option<&ServerApi>,
    user_principal: Option<String>,
    service_principal: String,
    source: &str,
) -> Result<()> {
    let (mut authenticator, initial_token) =
        GssapiAuthenticator::init(user_principal, service_principal).await?;

    let command = SaslStart::new(
        source.to_string(),
        crate::client::auth::AuthMechanism::Gssapi,
        initial_token,
        server_api.cloned(),
    )
    .into_command()?;

    let response_doc = conn.send_message(command).await?;
    let sasl_response =
        SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

    let mut conversation_id = Some(sasl_response.conversation_id);
    let mut payload = sasl_response.payload;

    // Limit number of auth challenge steps (typically, only one step is needed, however
    // different configurations may require more).
    for _ in 0..10 {
        let challenge = payload.as_slice();
        let output_token = authenticator.step(challenge).await?;

        // The step may return None, which is a valid final step. We still need to
        // send a saslContinue command, so we send an empty payload if there is no
        // token.
        let token = output_token.unwrap_or(vec![]);
        let command = SaslContinue::new(
            source.to_string(),
            conversation_id.clone().unwrap(),
            token,
            server_api.cloned(),
        )
        .into_command();

        let response_doc = conn.send_message(command).await?;
        let sasl_response =
            SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

        conversation_id = Some(sasl_response.conversation_id);
        payload = sasl_response.payload;

        // Although unlikely, there are cases where authentication can be done
        // at this point.
        if sasl_response.done {
            return Ok(());
        }

        // The authenticator is considered "complete" when the Kerberos auth
        // process is done. However, this is not the end of the full auth flow.
        // We no longer need to issue challenges to the authenticator, so we
        // break the loop and continue with the rest of the flow.
        if authenticator.is_complete() {
            break;
        }
    }

    let output_token = authenticator.do_unwrap_wrap(payload.as_slice())?;
    let command = SaslContinue::new(
        source.to_string(),
        conversation_id.unwrap(),
        output_token,
        server_api.cloned(),
    )
    .into_command();

    let response_doc = conn.send_message(command).await?;
    let sasl_response =
        SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

    if sasl_response.done {
        Ok(())
    } else {
        Err(Error::authentication_error(
            GSSAPI_STR,
            "GSSAPI authentication failed after 10 attempts",
        ))
    }
}

struct GssapiAuthenticator {
    pending_ctx: Option<PendingClientCtx>,
    established_ctx: Option<ClientCtx>,
    user_principal: Option<String>,
    is_complete: bool,
}

impl GssapiAuthenticator {
    // Initialize the GssapiAuthenticator by creating a PendingClientCtx and
    // getting an initial token to send to the server.
    async fn init(
        user_principal: Option<String>,
        service_principal: String,
    ) -> Result<(Self, Vec<u8>)> {
        let (pending_ctx, initial_token) = ClientCtx::new(
            InitiateFlags::empty(),
            user_principal.as_deref(),
            &service_principal,
            None, // No channel bindings
        )
        .map_err(|e| {
            Error::authentication_error(
                GSSAPI_STR,
                &format!("Failed to initialize GSSAPI context: {e}"),
            )
        })?;

        Ok((
            Self {
                pending_ctx: Some(pending_ctx),
                established_ctx: None,
                user_principal,
                is_complete: false,
            },
            initial_token.to_vec(),
        ))
    }

    // Issue the server provided token to the client context. If the ClientCtx
    // is established, an optional final token that must be sent to the server
    // may be returned; otherwise another token to pass to the server is
    // returned and the client context remains in the pending state.
    async fn step(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
        if challenge.is_empty() {
            Err(Error::authentication_error(
                GSSAPI_STR,
                "Expected challenge data for GSSAPI continuation",
            ))
        } else if let Some(pending_ctx) = self.pending_ctx.take() {
            match pending_ctx.step(challenge).map_err(|e| {
                Error::authentication_error(GSSAPI_STR, &format!("GSSAPI step failed: {e}"))
            })? {
                Step::Finished((ctx, token)) => {
                    self.is_complete = true;
                    self.established_ctx = Some(ctx);
                    Ok(token.map(|t| t.to_vec()))
                }
                Step::Continue((ctx, token)) => {
                    self.pending_ctx = Some(ctx);
                    Ok(Some(token.to_vec()))
                }
            }
        } else {
            Err(Error::authentication_error(
                GSSAPI_STR,
                "Authentication context not initialized",
            ))
        }
    }

    // Perform the final step of Kerberos authentication by gss_unwrap-ing the
    // final server challenge, then wrapping the protocol bytes + user principal.
    // The resulting token must be sent to the server.
    fn do_unwrap_wrap(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        if let Some(mut established_ctx) = self.established_ctx.take() {
            let _ = established_ctx.unwrap(payload).map_err(|e| {
                Error::authentication_error(GSSAPI_STR, &format!("GSSAPI unwrap failed: {e}"))
            })?;

            if let Some(user_principal) = self.user_principal.take() {
                let bytes: &[u8] = &[0x1, 0x0, 0x0, 0x0];
                let bytes = [bytes, user_principal.as_bytes()].concat();
                let output_token = established_ctx.wrap(false, bytes.as_slice()).map_err(|e| {
                    Error::authentication_error(GSSAPI_STR, &format!("GSSAPI wrap failed: {e}"))
                })?;
                Ok(output_token.to_vec())
            } else {
                Err(Error::authentication_error(
                    GSSAPI_STR,
                    "User principal not specified",
                ))
            }
        } else {
            Err(Error::authentication_error(
                GSSAPI_STR,
                "Authentication context not established",
            ))
        }
    }

    fn is_complete(&self) -> bool {
        self.is_complete
    }
}
