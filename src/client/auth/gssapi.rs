use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};
use sspi::{
    AcquireCredentialsHandleResult, BufferType, ClientRequestFlags, CredentialsBuffers,
    DataRepresentation, InitializeSecurityContextResult, Kerberos, KerberosConfig, SecurityBuffer,
    SecurityStatus, Sspi, SspiImpl, Username,
};

use crate::{
    bson::Bson,
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential, GSSAPI_STR,
        },
        options::ServerApi,
    },
    cmap::Connection,
    error::{Error, Result},
    options::ResolverConfig,
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
    resolver_config: Option<&ResolverConfig>,
) -> Result<()> {
    let properties = GssapiProperties::from_credential(credential)?;

    let conn_host = conn.address.host().to_string();
    let hostname = properties.service_host.as_ref().unwrap_or(&conn_host);
    let hostname = canonicalize_hostname(
        hostname,
        &properties.canonicalize_host_name,
        resolver_config,
    )
    .await?;

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

async fn canonicalize_hostname(
    hostname: &str,
    mode: &CanonicalizeHostName,
    resolver_config: Option<&ResolverConfig>,
) -> Result<String> {
    if mode == &CanonicalizeHostName::None {
        return Ok(hostname.to_string());
    }

    let resolver =
        crate::runtime::AsyncResolver::new(resolver_config.map(|c| c.inner.clone())).await?;

    let hostname = match mode {
        CanonicalizeHostName::Forward => {
            let lookup_records = resolver.cname_lookup(hostname).await?;

            if !lookup_records.records().is_empty() {
                // As long as there is a record, we can return the original hostname.
                // Although the spec says to return the canonical name, this is not
                // done by any drivers in practice since the majority of them use
                // libraries that do not follow CNAME chains. Also, we do not want to
                // use the canonical name since it will likely differ from the input
                // name, and the use of the input name is required for the service
                // principal to be accepted by the GSSAPI auth flow.
                hostname.to_lowercase().to_string()
            } else {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("No addresses found for hostname: {hostname}"),
                ));
            }
        }
        CanonicalizeHostName::ForwardAndReverse => {
            // forward lookup
            let ips = resolver.ip_lookup(hostname).await?;

            if let Some(first_address) = ips.iter().next() {
                // reverse lookup
                match resolver.reverse_lookup(first_address).await {
                    Ok(reverse_lookup) => {
                        if let Some(name) = reverse_lookup.iter().next() {
                            name.to_lowercase().to_string()
                        } else {
                            hostname.to_lowercase()
                        }
                    }
                    Err(_) => hostname.to_lowercase(),
                }
            } else {
                return Err(Error::authentication_error(
                    GSSAPI_STR,
                    &format!("No addresses found for hostname: {hostname}"),
                ));
            }
        }
        CanonicalizeHostName::None => unreachable!(),
    };

    // Sometimes reverse lookup results in a trailing "." since that is the correct
    // way to present a FQDN. However, GSSAPI rejects the trailing "." so we remove
    // it here manually.
    let hostname = hostname.trim_end_matches(".");

    Ok(hostname.to_string())
}

async fn windows_sspi(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    resolver_config: Option<&ResolverConfig>,
) -> Result<()> {
    let properties = GssapiProperties::from_credential(credential)?;

    let conn_host = conn.address.host().to_string();
    let hostname = properties.service_host.as_ref().unwrap_or(&conn_host);
    let hostname = canonicalize_hostname(
        hostname,
        &properties.canonicalize_host_name,
        resolver_config,
    )
    .await?;

    // Configuration
    let kerberos_config = KerberosConfig::new("", "".to_string());
    let mut kerberos = Kerberos::new_client_from_config(kerberos_config).map_err(|e| {
        Error::authentication_error(
            GSSAPI_STR,
            &format!("Failed to create SSPI Kerberos client: {e}"),
        )
    })?;

    // Acquire Creds
    let mut acq_creds_handle_result = get_cred_handle(&mut kerberos, credential)?;

    let mut conversation_id = None;
    let mut input_token: Vec<u8> = vec![];
    let source = credential.source.as_deref().unwrap_or("$external");

    loop {
        let (output_token, status) = step(
            &mut kerberos,
            &mut acq_creds_handle_result.credentials_handle,
            input_token.as_slice(),
            credential.username.clone(),
            properties.clone(),
            &hostname,
        )?;
        if status == SecurityStatus::ContinueNeeded || status == SecurityStatus::Ok {
            let command = if conversation_id.is_none() {
                SaslStart::new(
                    source.to_string(),
                    crate::client::auth::AuthMechanism::Gssapi,
                    output_token,
                    server_api.cloned(),
                )
                .into_command()?
            } else {
                SaslContinue::new(
                    source.to_string(),
                    conversation_id.clone().unwrap(),
                    output_token,
                    server_api.cloned(),
                )
                .into_command()
            };

            let response_doc = conn.send_message(command).await?;
            let sasl_response =
                SaslResponse::parse(GSSAPI_STR, response_doc.auth_response_body(GSSAPI_STR)?)?;

            conversation_id = Some(sasl_response.conversation_id);
            input_token = sasl_response.payload;

            if sasl_response.done {
                return Ok(())
            }
        } else {
            return Err(Error::authentication_error(
                GSSAPI_STR,
                &format!("problem authenticating; status = {status:?}"),
            ));
        }
    }

    Ok(())
}

pub(crate) fn get_cred_handle(
    kerberos: &mut Kerberos,
    credential: &Credential,
) -> Result<AcquireCredentialsHandleResult<Option<CredentialsBuffers>>> {
    let mut acq_creds_handle_result = kerberos
        .acquire_credentials_handle()
        .with_credential_use(sspi::CredentialUse::Outbound);

    if let Some(pwd) = credential.password.as_ref() {
        if let Some(username) = credential.username.as_ref() {
            let identity = sspi::AuthIdentity {
                username: Username::parse(username).map_err(|e| {
                    Error::authentication_error(
                        GSSAPI_STR,
                        &format!("Failed to parse user principal: {e}"),
                    )
                })?,
                password: pwd.into(),
            };

            acq_creds_handle_result = acq_creds_handle_result.with_auth_data(&identity.into());
        } else {
            return Err(Error::authentication_error(
                GSSAPI_STR,
                "Username required but not specified",
            ));
        }
    }

    acq_creds_handle_result.execute(kerberos).map_err(|e| {
        Error::authentication_error(
            GSSAPI_STR,
            &format!("Failed to acquire credentials handle: {e:?}"),
        )
    })
}

fn step_helper(
    kerberos: &mut Kerberos,
    cred_handle: &mut <Kerberos as SspiImpl>::CredentialsHandle,
    input_buffer: &mut [SecurityBuffer],
    output_buffer: &mut [SecurityBuffer],
    user_principal: Option<String>,
    properties: GssapiProperties,
    hostname: &str,
) -> Result<InitializeSecurityContextResult> {
    // todo: consider not only refactoring this duplicated code into a helper function,
    //       but possibly updating GssapiProperties::from-credential to create the service principal for me
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
    let mut builder = kerberos
        .initialize_security_context()
        .with_credentials_handle(cred_handle)
        .with_context_requirements(ClientRequestFlags::MUTUAL_AUTH)
        .with_target_data_representation(DataRepresentation::Native)
        .with_target_name(&service_principal)
        .with_input(input_buffer)
        .with_output(output_buffer);

    kerberos
        .initialize_security_context_impl(&mut builder)
        .map_err(|e| {
            Error::authentication_error(
                GSSAPI_STR,
                &format!("Failed to initialize security context: {e}"),
            )
        })?
        .resolve_to_result()
        .map_err(|e| {
            Error::authentication_error(GSSAPI_STR, &format!("Failed to resolve to result: {e}"))
        })
}

pub fn step(
    kerberos: &mut Kerberos,
    cred_handle: &mut <Kerberos as SspiImpl>::CredentialsHandle,
    input_token: &[u8],
    user_principal: Option<String>,
    properties: GssapiProperties,
    hostname: &str,
) -> Result<(Vec<u8>, SecurityStatus)> {
    let mut secure_input_buffer =
        vec![SecurityBuffer::new(input_token.to_vec(), BufferType::Token)];
    let mut secure_output_buffer = vec![SecurityBuffer::new(Vec::new(), BufferType::Token)];
    match step_helper(
        kerberos,
        cred_handle,
        &mut secure_input_buffer,
        &mut secure_output_buffer,
        user_principal,
        properties,
        hostname,
    ) {
        Ok(result) => {
            let output_buffer = secure_output_buffer[0].to_owned();
            Ok((output_buffer.buffer, result.status))
        }
        Err(e) => Err(Error::authentication_error(
            GSSAPI_STR,
            &format!("error stepping: {e:?}"),
        )),
    }
}
