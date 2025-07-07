use cross_krb5::{ClientCtx, InitiateFlags, K5Ctx, PendingClientCtx, Step};
use dns_lookup::getnameinfo;
use std::net::SocketAddr;

use crate::{
    bson::Bson,
    client::{
        auth::{
            sasl::{SaslContinue, SaslResponse, SaslStart},
            Credential,
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
const MECH_NAME: &str = "GSSAPI";

#[derive(Debug, Clone)]
pub(crate) struct GssapiProperties {
    pub service_name: String,
    pub canonicalize_host_name: CanonicalizeHostName,
    pub service_realm: Option<String>,
    pub service_host: Option<String>,
}

#[derive(Debug, Default, Clone)]
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
    let mut authenticator =
        GssapiAuthenticator::new(user_principal, properties.clone(), &hostname).await?;

    println!("start authenticator.init()");
    let output_token = authenticator.init().await?;
    println!("finish authenticator.init() - output_token = {output_token:?}");

    let source = credential.source.as_deref().unwrap_or("$external");

    let command = SaslStart::new(
        source.to_string(),
        crate::client::auth::AuthMechanism::Gssapi,
        output_token,
        server_api.cloned(),
    )
    .into_command();

    println!("start conn.send_message() - command = {command:?}");
    let response_doc = conn.send_message(command).await?.body()?;
    println!("finish conn.send_message() - response_doc = {response_doc:?}");
    let sasl_response = SaslResponse::parse("GSSAPI", response_doc)?;

    let mut conversation_id = Some(sasl_response.conversation_id);
    let mut payload = sasl_response.payload;

    for i in 0..10 {
        println!("loop iteration {i}");
        let challenge = payload.as_slice();
        println!("\tstart authenticator.step() - challenge = {challenge:?}");
        let output_token = authenticator.step(challenge).await?;
        println!("\tfinish authenticator.step() - output_token = {output_token:?}");

        let token = output_token.unwrap_or(vec![]);
        let command = SaslContinue::new(
            source.to_string(),
            conversation_id.clone().unwrap(),
            token,
            server_api.cloned(),
        )
        .into_command();

        println!("\tstart conn.send_message() - command = {command:?}");
        let response_doc = conn.send_message(command).await?.body()?;
        println!("\tfinish conn.send_message() - response_doc = {response_doc:?}");
        let sasl_response = SaslResponse::parse("GSSAPI", response_doc)?;

        conversation_id = Some(sasl_response.conversation_id);
        payload = sasl_response.payload;

        if sasl_response.done {
            return Ok(());
        }

        if authenticator.is_complete() {
            break;
        }
    }

    println!("starting do_unwrap_wrap - payload = {payload:?}");
    if let Some(output_token) = authenticator.do_unwrap_wrap(payload.as_slice())? {
        println!("finished do_unwrap_wrap - output_token = {output_token:?}");
        let command = SaslContinue::new(
            source.to_string(),
            conversation_id.unwrap(),
            output_token,
            server_api.cloned(),
        )
        .into_command();

        println!("\tstart conn.send_message() - command = {command:?}");
        let response_doc = conn.send_message(command).await?.body()?;
        println!("\tfinish conn.send_message() - response_doc = {response_doc:?}");
        let sasl_response = SaslResponse::parse("GSSAPI", response_doc)?;

        if sasl_response.done {
            println!("got done");
            return Ok(());
        }
    }

    Err(Error::authentication_error(
        "GSSAPI",
        "GSSAPI authentication failed after 10 attempts",
    ))
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
            if let Some(service_name) = mechanism_properties.get(SERVICE_NAME) {
                if let Bson::String(name) = service_name {
                    properties.service_name = name.clone();
                }
            }

            if let Some(canonicalize) = mechanism_properties.get(CANONICALIZE_HOST_NAME) {
                properties.canonicalize_host_name = match canonicalize {
                    Bson::String(s) => match s.as_str() {
                        "none" => CanonicalizeHostName::None,
                        "forward" => CanonicalizeHostName::Forward,
                        "forwardAndReverse" => CanonicalizeHostName::ForwardAndReverse,
                        _ => return Err(Error::authentication_error(
                            MECH_NAME,
                            format!("Invalid CANONICALIZE_HOST_NAME value: {}. Valid values are 'none', 'forward', 'forwardAndReverse'", s).as_str()
                        )),
                    },
                    Bson::Boolean(true) => CanonicalizeHostName::ForwardAndReverse,
                    Bson::Boolean(false) => CanonicalizeHostName::None,
                    _ => return Err(Error::authentication_error(
                        MECH_NAME,
                        "CANONICALIZE_HOST_NAME must be a string or boolean",
                    )),
                };
            }

            if let Some(service_realm) = mechanism_properties.get(SERVICE_REALM) {
                if let Bson::String(realm) = service_realm {
                    properties.service_realm = Some(realm.clone());
                }
            }

            if let Some(service_host) = mechanism_properties.get(SERVICE_HOST) {
                if let Bson::String(host) = service_host {
                    properties.service_host = Some(host.clone());
                }
            }
        }

        Ok(properties)
    }
}

struct GssapiAuthenticator {
    pending_ctx: Option<PendingClientCtx>,
    established_ctx: Option<ClientCtx>,
    service_principal: String,
    user_principal: Option<String>,
    is_complete: bool,
}

impl GssapiAuthenticator {
    async fn new(
        user_principal: Option<String>,
        properties: GssapiProperties,
        hostname: &str,
    ) -> Result<Self> {
        let service_name: &str = properties.service_name.as_ref();
        let mut service_principal = format!("{}/{}", service_name, hostname);
        if let Some(service_realm) = properties.service_realm.as_ref() {
            service_principal = format!("{}@{}", service_principal, service_realm);
        } else if let Some(user_principal) = user_principal.as_ref() {
            if let Some(idx) = user_principal.find('@') {
                // If no SERVICE_REALM was specified, use realm specified in the
                // username. Note that `realm` starts with '@'.
                let (_, realm) = user_principal.split_at(idx);
                service_principal = format!("{}{}", service_principal, realm);
            }
        }

        Ok(Self {
            pending_ctx: None,
            established_ctx: None,
            service_principal,
            user_principal,
            is_complete: false,
        })
    }

    async fn init(&mut self) -> Result<Vec<u8>> {
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
        Ok(initial_token.to_vec())
    }

    async fn step(&mut self, challenge: &[u8]) -> Result<Option<Vec<u8>>> {
        if challenge.is_empty() {
            Err(Error::authentication_error(
                "GSSAPI",
                "Expected challenge data for GSSAPI continuation",
            ))
        } else {
            if let Some(pending_ctx) = self.pending_ctx.take() {
                println!("\t\tabout to call pending_ctx.step()");
                match pending_ctx.step(challenge).map_err(|e| {
                    Error::authentication_error("GSSAPI", &format!("GSSAPI step failed: {}", e))
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
                    "GSSAPI",
                    "Authentication context not initialized",
                ))
            }
        }
    }

    fn do_unwrap_wrap(&mut self, payload: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(mut established_ctx) = self.established_ctx.take() {
            let _ = established_ctx.unwrap(payload).map_err(|e| {
                Error::authentication_error("GSSAPI", &format!("GSSAPI unwrap failed: {}", e))
            })?;

            // // todo: instead of wrapping the user principal, try wrapping the byte array
            // //    [ 0x1, 0x0, 0x0, 0x0 ]
            // // bytesReceivedFromServer = new byte[length];
            // // bytesReceivedFromServer[0] = 0x1; // NO_PROTECTION
            // // bytesReceivedFromServer[1] = 0x0; // NO_PROTECTION
            // // bytesReceivedFromServer[2] = 0x0; // NO_PROTECTION
            // // bytesReceivedFromServer[3] = 0x0; // NO_PROTECTION
            //
            // let bytes: &[u8] = &[0x00, 0xFF, 0xFF, 0xFF];
            // let output_token = established_ctx.wrap(true, bytes).map_err(|e| {
            //     Error::authentication_error("GSSAPI", &format!("GSSAPI wrap failed: {}", e))
            // })?;
            // Ok(Some(output_token.to_vec()))

            if let Some(user_principal) = self.user_principal.take() {
                let bytes: &[u8] = &[0x1, 0x0, 0x0, 0x0];
                // let bytes: &[u8] = &[0x00, 0xFF, 0xFF, 0xFF];
                let bytes = [bytes, user_principal.as_bytes()].concat();
                println!("user principal: {user_principal}");
                println!("user principal bytes: {:?}", user_principal.as_bytes());
                let output_token = established_ctx.wrap(false, bytes.as_slice()).map_err(|e| {
                    Error::authentication_error("GSSAPI", &format!("GSSAPI wrap failed: {}", e))
                })?;
                Ok(Some(output_token.to_vec()))
            } else {
                Err(Error::authentication_error(
                    "GSSAPI",
                    "User principal not specified",
                ))
            }
        } else {
            Err(Error::authentication_error(
                "GSSAPI",
                "Authentication context not established",
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
