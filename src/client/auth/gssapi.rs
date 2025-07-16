use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};
use hickory_resolver::{
    proto::rr::{RData, RecordType},
    TokioAsyncResolver,
};

use crate::{
    bson::Bson,
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential,
            GSSAPI_STR,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
};

const SERVICE_NAME: &str = "SERVICE_NAME";
const CANONICALIZE_HOST_NAME: &str = "CANONICALIZE_HOST_NAME";
const SERVICE_REALM: &str = "SERVICE_REALM";
const SERVICE_HOST: &str = "SERVICE_HOST";

#[derive(Debug, Clone)]
pub(crate) struct GssapiProperties {
    pub service_name: String,
    pub canonicalize_host_name: CanonicalizeHostName,
    pub service_realm: Option<String>,
    pub service_host: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) enum CanonicalizeHostName {
    #[default]
    None,
    Forward,
    ForwardAndReverse,
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
) -> Result<()> {
    let properties = GssapiProperties::from_credential(credential)?;

    let conn_host = conn.address.host().to_string();
    let hostname = properties.service_host.as_ref().unwrap_or(&conn_host);
    let hostname = canonicalize_hostname(hostname, &properties.canonicalize_host_name).await?;

    let user_principal = credential.username.clone();
    let (mut authenticator, initial_token) =
        GssapiAuthenticator::init(user_principal, properties.clone(), &hostname).await?;

    let source = credential.source.as_deref().unwrap_or("$external");

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

impl GssapiProperties {
    pub fn from_credential(credential: &Credential) -> Result<Self> {
        let mut properties = GssapiProperties {
            service_name: "mongodb".to_string(),
            canonicalize_host_name: CanonicalizeHostName::None,
            service_realm: None,
            service_host: None,
        };

        if let Some(mechanism_properties) = &credential.mechanism_properties {
            if let Some(Bson::String(name)) = mechanism_properties.get(SERVICE_NAME) {
                properties.service_name = name.clone();
            }

            if let Some(canonicalize) = mechanism_properties.get(CANONICALIZE_HOST_NAME) {
                properties.canonicalize_host_name = match canonicalize {
                    Bson::String(s) => match s.as_str() {
                        "none" => CanonicalizeHostName::None,
                        "forward" => CanonicalizeHostName::Forward,
                        "forwardAndReverse" => CanonicalizeHostName::ForwardAndReverse,
                        _ => {
                            return Err(Error::authentication_error(
                                GSSAPI_STR,
                                format!(
                                    "Invalid CANONICALIZE_HOST_NAME value: {s}. Valid values are \
                                     'none', 'forward', 'forwardAndReverse'",
                                )
                                .as_str(),
                            ))
                        }
                    },
                    Bson::Boolean(true) => CanonicalizeHostName::ForwardAndReverse,
                    Bson::Boolean(false) => CanonicalizeHostName::None,
                    _ => {
                        return Err(Error::authentication_error(
                            GSSAPI_STR,
                            "CANONICALIZE_HOST_NAME must be a string or boolean",
                        ))
                    }
                };
            }

            if let Some(Bson::String(realm)) = mechanism_properties.get(SERVICE_REALM) {
                properties.service_realm = Some(realm.clone());
            }

            if let Some(Bson::String(host)) = mechanism_properties.get(SERVICE_HOST) {
                properties.service_host = Some(host.clone());
            }
        }

        Ok(properties)
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
        properties: GssapiProperties,
        hostname: &str,
    ) -> Result<(Self, Vec<u8>)> {
        let service_name: &str = properties.service_name.as_ref();
        let mut service_principal = format!("{service_name}/{hostname}");
        if let Some(service_realm) = properties.service_realm.as_ref() {
            service_principal = format!("{service_principal}@{service_realm}");
        } else if let Some(user_principal) = user_principal.as_ref() {
            if let Some(idx) = user_principal.find('@') {
                // If no SERVICE_REALM was specified, use realm specified in the
                // username. Note that `realm` starts with '@'.
                let (_, realm) = user_principal.split_at(idx);
                service_principal = format!("{service_principal}{realm}");
            }
        }

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

async fn canonicalize_hostname(hostname: &str, mode: &CanonicalizeHostName) -> Result<String> {
    if mode == &CanonicalizeHostName::None {
        return Ok(hostname.to_string());
    }

    let resolver = TokioAsyncResolver::tokio_from_system_conf().map_err(|e| {
        Error::authentication_error(
            GSSAPI_STR,
            &format!("Failed to initialize hostname Resolver for canonicalization: {e:?}"),
        )
    })?;

    match mode {
        CanonicalizeHostName::Forward => {
            let lookup_records =
                resolver
                    .lookup(hostname, RecordType::CNAME)
                    .await
                    .map_err(|e| {
                        Error::authentication_error(
                            GSSAPI_STR,
                            &format!("Failed to look up hostname for canonicalization: {e:?}"),
                        )
                    })?;

            if let Some(first_record) = lookup_records.records().first() {
                if let Some(RData::CNAME(cname)) = first_record.data() {
                    Ok(cname.to_lowercase().to_string())
                } else {
                    Ok(hostname.to_string())
                }
            } else {
                Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("No addresses found for hostname: {hostname}"),
                ))
            }
        }
        CanonicalizeHostName::ForwardAndReverse => {
            // forward lookup
            let ips = resolver.lookup_ip(hostname).await.map_err(|e| {
                Error::authentication_error(
                    GSSAPI_STR,
                    &format!("Failed to look up hostname for canonicalization: {e:?}"),
                )
            })?;

            if let Some(first_address) = ips.iter().next() {
                // reverse lookup
                match resolver.reverse_lookup(first_address).await {
                    Ok(reverse_lookup) => {
                        if let Some(name) = reverse_lookup.iter().next() {
                            Ok(name.to_lowercase().to_string())
                        } else {
                            Ok(hostname.to_lowercase())
                        }
                    }
                    Err(_) => Ok(hostname.to_lowercase()),
                }
            } else {
                Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("No addresses found for hostname: {hostname}"),
                ))
            }
        }
        CanonicalizeHostName::None => unreachable!(),
    }
}
