use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};

use crate::{
    client::auth::GSSAPI_STR,
    error::{Error, Result},
};

pub(super) struct GssapiAuthenticator {
    pending_ctx: Option<PendingClientCtx>,
    established_ctx: Option<ClientCtx>,
    user_principal: String,
    is_complete: bool,
}

impl GssapiAuthenticator {
    // Initialize the GssapiAuthenticator by creating a PendingClientCtx and
    // getting an initial token to send to the server.
    pub(super) fn init(
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

        let user_principal = user_principal.ok_or_else(|| {
            Error::authentication_error(GSSAPI_STR, "User principal not specified")
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
    pub(super) fn step(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
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
    pub(super) fn do_unwrap_wrap(&mut self, payload: &[u8]) -> Result<Vec<u8>> {
        if let Some(mut established_ctx) = self.established_ctx.take() {
            let _ = established_ctx.unwrap(payload).map_err(|e| {
                Error::authentication_error(GSSAPI_STR, &format!("GSSAPI unwrap failed: {e}"))
            })?;

            let bytes: &[u8] = &[0x1, 0x0, 0x0, 0x0];
            let bytes = [bytes, self.user_principal.as_bytes()].concat();
            let output_token = established_ctx.wrap(false, bytes.as_slice()).map_err(|e| {
                Error::authentication_error(GSSAPI_STR, &format!("GSSAPI wrap failed: {e}"))
            })?;
            Ok(output_token.to_vec())
        } else {
            Err(Error::authentication_error(
                GSSAPI_STR,
                "Authentication context not established",
            ))
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.is_complete
    }
}
