#![cfg(feature = "gssapi-auth")]

use cross_krb5::{ClientCtx, InitiateFlags, Step};
use dns_lookup::getnameinfo;
use std::net::SocketAddr;

use crate::{
    bson::{Bson, Document},
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential,
        },
        options::ServerApi,
    },
    cmap::{Command, Connection},
    error::{Error, ErrorKind, Result},
};
#[derive(Debug, Clone)]
pub(crate) struct GssapiProperties {
    pub service_name: String,
    pub canonicalize_host_name: CanonicalizeHostName,
    pub service_realm: Option<String>,
    pub service_host: Option<String>,
    pub kdc_url: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) enum CanonicalizeHostName {
    None,
    Forward,
    ForwardAndReverse,
}

impl Default for CanonicalizeHostName {
    fn default() -> Self {
        CanonicalizeHostName::None
    }
}

impl GssapiProperties {
    pub fn from_credential(credential: &Credential) -> Result<Self> {
        let mut properties = GssapiProperties {
            service_name: "mongodb".to_string(),
            canonicalize_host_name: CanonicalizeHostName::None,
            service_realm: None,
            service_host: None,
            kdc_url: None,
        };

        if let Some(mechanism_properties) = &credential.mechanism_properties {
            if let Some(service_name) = mechanism_properties.get("SERVICE_NAME") {
                if let Bson::String(name) = service_name {
                    properties.service_name = name.clone();
                }
            }

            if let Some(canonicalize) = mechanism_properties.get("CANONICALIZE_HOST_NAME") {
                properties.canonicalize_host_name = match canonicalize {
                    Bson::String(s) => match s.as_str() {
                        "none" => CanonicalizeHostName::None,
                        "forward" => CanonicalizeHostName::Forward,
                        "forwardAndReverse" => CanonicalizeHostName::ForwardAndReverse,
                        _ => return Err(ErrorKind::InvalidArgument {
                            message: format!("Invalid CANONICALIZE_HOST_NAME value: {}. Valid values are 'none', 'forward', 'forwardAndReverse'", s),
                        }.into()),
                    },
                    Bson::Boolean(true) => CanonicalizeHostName::ForwardAndReverse,
                    Bson::Boolean(false) => CanonicalizeHostName::None,
                    _ => return Err(ErrorKind::InvalidArgument {
                        message: "CANONICALIZE_HOST_NAME must be a string or boolean".to_string(),
                    }.into()),
                };
            }

            if let Some(service_realm) = mechanism_properties.get("SERVICE_REALM") {
                if let Bson::String(realm) = service_realm {
                    properties.service_realm = Some(realm.clone());
                }
            }

            if let Some(service_host) = mechanism_properties.get("SERVICE_HOST") {
                if let Bson::String(host) = service_host {
                    properties.service_host = Some(host.clone());
                }
            }

            if let Some(kdc_url) = mechanism_properties.get("KDC_URL") {
                if let Bson::String(url) = kdc_url {
                    properties.kdc_url = Some(url.clone());
                }
            }
        }

        Ok(properties)
    }
}

struct GssapiAuthenticator {
    pending_ctx: Option<cross_krb5::PendingClientCtx>,
    client_ctx: Option<ClientCtx>,
    service_principal: String,
    is_complete: bool,
    properties: GssapiProperties,
    user_principal: Option<String>,
}

impl GssapiAuthenticator {
    async fn new(
        credential: &Credential,
        properties: GssapiProperties,
        hostname: &str,
    ) -> Result<Self> {
        let service_name: &str = properties.service_name.as_ref();
        let service_principal = format!("{}/{}", service_name, hostname);

        Ok(Self {
            pending_ctx: None,
            client_ctx: None,
            service_principal,
            is_complete: false,
            properties,
            user_principal: credential.username.clone(),
        })
    }

    async fn step(&mut self, challenge: Option<&[u8]>) -> Result<Option<Vec<u8>>> {
        if self.pending_ctx.is_none() && self.client_ctx.is_none() {
            let (pending_ctx, initial_token) = ClientCtx::new(
                InitiateFlags::empty(),
                self.user_principal.as_deref(), // Use provided credentials
                &self.service_principal,
                None, // No channel bindings
            )
            .map_err(|e| {
                Error::authentication_error(
                    "GSSAPI",
                    &format!("Failed to initialize GSSAPI context: {}", e),
                )
            })?;

            self.pending_ctx = Some(pending_ctx);
            return Ok(Some(initial_token.to_vec()));
        }

        if let Some(challenge_data) = challenge {
            if let Some(pending_ctx) = self.pending_ctx.take() {
                match pending_ctx.step(challenge_data).map_err(|e| {
                    Error::authentication_error("GSSAPI", &format!("GSSAPI step failed: {}", e))
                })? {
                    Step::Finished((ctx, token)) => {
                        self.client_ctx = Some(ctx);
                        self.is_complete = true;
                        Ok(token.map(|t| t.to_vec()))
                    }
                    Step::Continue((ctx, token)) => {
                        self.pending_ctx = Some(ctx);
                        Ok(Some(token.to_vec()))
                    }
                }
            } else {
                Err(Error::authentication_error(
                    "GSSAPI",
                    "Authentication context not initialized",
                ))
            }
        } else {
            Err(Error::authentication_error(
                "GSSAPI",
                "Expected challenge data for GSSAPI continuation",
            ))
        }
    }

    fn is_complete(&self) -> bool {
        self.is_complete
    }
}

async fn canonicalize_hostname(hostname: &str, mode: &CanonicalizeHostName) -> Result<String> {
    match mode {
        CanonicalizeHostName::None => Ok(hostname.to_string()),
        CanonicalizeHostName::Forward => {
            let (canonical_host, _address) = resolve_hostname_with_canonical(hostname).await?;
            Ok(canonical_host.to_lowercase())
        }
        CanonicalizeHostName::ForwardAndReverse => {
            let (canonical_host, address) = resolve_hostname_with_canonical(hostname).await?;
            match perform_reverse_dns_lookup(address).await {
                Ok(reversed_hostname) => Ok(reversed_hostname.to_lowercase()),
                Err(_) => Ok(canonical_host.to_lowercase()),
            }
        }
    }
}

async fn resolve_hostname_with_canonical(hostname: &str) -> Result<(String, std::net::IpAddr)> {
    match tokio::net::lookup_host((hostname, 0)).await {
        Ok(mut addrs) => {
            if let Some(addr) = addrs.next() {
                Ok((hostname.to_string(), addr.ip()))
            } else {
                return Err(Error::authentication_error(
                    "GSSAPI",
                    &format!("No addresses found for hostname '{}'", hostname),
                ));
            }
        }
        Err(e) => Err(Error::authentication_error(
            "GSSAPI",
            &format!("DNS resolution failed for hostname '{}': {}", hostname, e),
        )),
    }
}

async fn perform_reverse_dns_lookup(ip: std::net::IpAddr) -> Result<String> {
    let sockaddr = SocketAddr::new(ip, 0);
    match tokio::task::spawn_blocking(move || getnameinfo(&sockaddr, 0)).await {
        Ok(Ok((hostname, _))) => Ok(hostname),
        Ok(Err(_)) | Err(_) => Err(Error::authentication_error(
            "GSSAPI",
            "Reverse DNS lookup failed",
        )),
    }
}

pub(crate) async fn authenticate_stream(
    conn: &mut Connection,
    credential: &Credential,
    server_api: Option<&ServerApi>,
    _server_first: impl Into<Option<Document>>,
) -> Result<()> {
    let properties = GssapiProperties::from_credential(credential)?;
    let hostname =
        canonicalize_hostname(&conn.address().host(), &properties.canonicalize_host_name).await?;
    let _service_host = properties.service_host.as_ref().unwrap_or(&hostname);

    let mut authenticator =
        GssapiAuthenticator::new(credential, properties.clone(), &hostname).await?;

    let source = credential.source.as_deref().unwrap_or("$external");
    let mut conversation_id = None;
    let mut payload = Vec::new();

    for _ in 0..10 {
        let challenge = if payload.is_empty() {
            None
        } else {
            Some(payload.as_slice())
        };
        let output_token = authenticator.step(challenge).await?;

        if let Some(token) = output_token {
            let command = if conversation_id.is_none() {
                SaslStart::new(
                    source.to_string(),
                    crate::client::auth::AuthMechanism::Gssapi,
                    token,
                    server_api.cloned(),
                )
                .into_command()
            } else {
                SaslContinue::new(
                    source.to_string(),
                    conversation_id.clone().unwrap(),
                    token,
                    server_api.cloned(),
                )
                .into_command()
            };

            let response_doc = conn.send_message(command).await?.body()?;
            let sasl_response = SaslResponse::parse("GSSAPI", response_doc)?;

            conversation_id = Some(sasl_response.conversation_id);
            payload = sasl_response.payload;

            if sasl_response.done && authenticator.is_complete() {
                return Ok(());
            }
        } else if authenticator.is_complete() {
            return Ok(());
        }
    }

    Err(Error::authentication_error(
        "GSSAPI",
        "GSSAPI authentication failed after 10 attempts",
    ))
}

pub(crate) fn build_speculative_client_first(_credential: &Credential) -> Option<Command> {
    None
}
