#[cfg(test)]
mod test;

use std::sync::Arc;

use lazy_static::lazy_static;
use os_info::{Type, Version};

use crate::{
    bson::{doc, Bson, Document},
    client::auth::{ClientFirst, FirstRound},
    cmap::{options::ConnectionPoolOptions, Command, Connection, StreamDescription},
    compression::Compressor,
    error::{ErrorKind, Result},
    event::sdam::SdamEventHandler,
    hello::{hello_command, run_hello, HelloReply},
    options::{AuthMechanism, ClientOptions, Credential, DriverInfo, ServerApi},
    sdam::Topology,
};

#[cfg(all(feature = "tokio-runtime", not(feature = "tokio-sync")))]
const RUNTIME_NAME: &str = "tokio";

#[cfg(all(feature = "async-std-runtime", not(feature = "sync")))]
const RUNTIME_NAME: &str = "async-std";

#[cfg(feature = "sync")]
const RUNTIME_NAME: &str = "sync (with async-std)";

#[cfg(feature = "tokio-sync")]
const RUNTIME_NAME: &str = "sync (with tokio)";

#[derive(Clone, Debug)]
struct ClientMetadata {
    application: Option<AppMetadata>,
    driver: DriverMetadata,
    os: OsMetadata,
    platform: String,
}

#[derive(Clone, Debug)]
struct AppMetadata {
    name: String,
}

#[derive(Clone, Debug)]
struct DriverMetadata {
    name: String,
    version: String,
}

#[derive(Clone, Debug)]
struct OsMetadata {
    os_type: String,
    name: Option<String>,
    architecture: String,
    version: Option<String>,
}

impl From<ClientMetadata> for Bson {
    fn from(metadata: ClientMetadata) -> Self {
        let mut metadata_doc = Document::new();

        if let Some(application) = metadata.application {
            metadata_doc.insert("application", doc! { "name": application.name });
        }

        metadata_doc.insert(
            "driver",
            doc! {
                "name": metadata.driver.name,
                "version": metadata.driver.version,
            },
        );

        metadata_doc.insert("os", metadata.os);
        metadata_doc.insert("platform", metadata.platform);

        Bson::Document(metadata_doc)
    }
}

impl From<OsMetadata> for Bson {
    fn from(metadata: OsMetadata) -> Self {
        let mut doc = doc! { "type": metadata.os_type };

        if let Some(name) = metadata.name {
            doc.insert("name", name);
        }

        doc.insert("architecture", metadata.architecture);

        if let Some(version) = metadata.version {
            doc.insert("version", version);
        }

        Bson::Document(doc)
    }
}

lazy_static! {
    /// Contains the basic handshake information that can be statically determined. This document
    /// (potentially with additional fields added) can be cloned and put in the `client` field of
    /// the `hello` or legacy hello command.
    static ref BASE_CLIENT_METADATA: ClientMetadata = {
        let mut metadata = ClientMetadata {
            application: None,
            driver: DriverMetadata {
                name: "mongo-rust-driver".into(),
                version: env!("CARGO_PKG_VERSION").into(),
            },
            os: OsMetadata {
                os_type: std::env::consts::OS.into(),
                architecture: std::env::consts::ARCH.into(),
                name: None,
                version: None,
            },
            platform: format!("{} with {}", rustc_version_runtime::version_meta().short_version_string, RUNTIME_NAME),
        };

        let info = os_info::get();

        if info.os_type() != Type::Unknown {
            let version = info.version();

            if *version != Version::Unknown {
                metadata.os.version = Some(info.version().to_string());
            }
        }

        metadata
    };
}

/// Contains the logic needed to handshake a connection.
#[derive(Clone, Debug)]
pub(crate) struct Handshaker {
    /// The hello or legacy hello command to send when handshaking. This will always be identical
    /// given the same pool options, so it can be created at the time the Handshaker is created.
    command: Command,

    credential: Option<Credential>,

    // This field is not read without a compression feature flag turned on.
    #[allow(dead_code)]
    compressors: Option<Vec<Compressor>>,
}

impl Handshaker {
    /// Creates a new Handshaker.
    pub(crate) fn new(options: Option<HandshakerOptions>) -> Self {
        let mut metadata = BASE_CLIENT_METADATA.clone();
        let mut credential = None;

        let mut compressors = None;

        let mut command = hello_command(
            options.as_ref().and_then(|opts| opts.server_api.as_ref()),
            options.as_ref().and_then(|opts| opts.load_balanced.into()),
            None,
        );

        if let Some(options) = options {
            if let Some(app_name) = options.app_name {
                metadata.application = Some(AppMetadata { name: app_name });
            }

            if let Some(driver_info) = options.driver_info {
                metadata.driver.name.push('|');
                metadata.driver.name.push_str(&driver_info.name);

                if let Some(ref version) = driver_info.version {
                    metadata.driver.version.push('|');
                    metadata.driver.version.push_str(version);
                }

                if let Some(ref driver_info_platform) = driver_info.platform {
                    metadata.platform.push('|');
                    metadata.platform.push_str(driver_info_platform);
                }
            }

            if let Some(cred) = options.credential {
                cred.append_needed_mechanism_negotiation(&mut command.body);
                command.target_db = cred.resolved_source().to_string();
                credential = Some(cred);
            }

            if options.load_balanced {
                command.body.insert("loadBalanced", true);
            }

            // Add compressors to handshake.
            // See https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
            if let Some(ref compressors) = options.compressors {
                command.body.insert(
                    "compression",
                    compressors
                        .iter()
                        .map(|x| x.name())
                        .collect::<Vec<&'static str>>(),
                );
            }
            compressors = options.compressors;
        }

        command.body.insert("client", metadata);

        Self {
            command,
            credential,
            compressors,
        }
    }

    /// Handshakes a connection.
    pub(crate) async fn handshake(
        &self,
        conn: &mut Connection,
        topology: Option<&Topology>,
        handler: &Option<Arc<dyn SdamEventHandler>>,
    ) -> Result<HandshakeResult> {
        let mut command = self.command.clone();

        let client_first = set_speculative_auth_info(&mut command.body, self.credential.as_ref())?;

        let mut hello_reply = run_hello(conn, command, topology, handler).await?;

        if self.command.body.contains_key("loadBalanced")
            && hello_reply.command_response.service_id.is_none()
        {
            return Err(ErrorKind::IncompatibleServer {
                message: "Driver attempted to initialize in load balancing mode, but the server \
                          does not support this mode."
                    .to_string(),
            }
            .into());
        }
        conn.stream_description = Some(StreamDescription::from_hello_reply(&hello_reply));

        // Record the client's message and the server's response from speculative authentication if
        // the server did send a response.
        let first_round = client_first.and_then(|client_first| {
            hello_reply
                .command_response
                .speculative_authenticate
                .take()
                .map(|server_first| client_first.into_first_round(server_first))
        });

        // Check that the hello reply has a compressor list and unpack it
        if let (Some(server_compressors), Some(client_compressors)) = (
            hello_reply.command_response.compressors.as_ref(),
            self.compressors.as_ref(),
        ) {
            // Use the Client's first compressor choice that the server supports (comparing only on
            // enum variant)
            if let Some(compressor) = client_compressors
                .iter()
                .find(|c| server_compressors.iter().any(|x| c.name() == x))
            {
                // Without a feature flag turned on, the Compressor enum is empty which causes an
                // unreachable code warning.
                #[allow(unreachable_code)]
                // zlib compression level is already set
                {
                    conn.compressor = Some(compressor.clone());
                }
            }
        }

        Ok(HandshakeResult {
            hello_reply,
            first_round,
        })
    }
}

/// The information returned from the server as part of the handshake.
///
/// Also optionally includes the first round of speculative authentication
/// if applicable.
#[derive(Debug)]
pub(crate) struct HandshakeResult {
    /// The response from the server.
    pub(crate) hello_reply: HelloReply,

    /// The first round of speculative authentication, if applicable.
    pub(crate) first_round: Option<FirstRound>,
}

#[derive(Debug)]
pub(crate) struct HandshakerOptions {
    app_name: Option<String>,
    credential: Option<Credential>,
    compressors: Option<Vec<Compressor>>,
    driver_info: Option<DriverInfo>,
    server_api: Option<ServerApi>,
    load_balanced: bool,
}

impl From<ConnectionPoolOptions> for HandshakerOptions {
    fn from(options: ConnectionPoolOptions) -> Self {
        Self {
            app_name: options.app_name,
            compressors: options.compressors,
            credential: options.credential,
            driver_info: options.driver_info,
            server_api: options.server_api,
            load_balanced: options.load_balanced.unwrap_or(false),
        }
    }
}

impl From<ClientOptions> for HandshakerOptions {
    fn from(options: ClientOptions) -> Self {
        Self {
            app_name: options.app_name,
            compressors: options.compressors,
            credential: options.credential,
            driver_info: options.driver_info,
            server_api: options.server_api,
            load_balanced: options.load_balanced.unwrap_or(false),
        }
    }
}

/// Updates the handshake command document with the speculative authenitication info.
fn set_speculative_auth_info(
    command: &mut Document,
    credential: Option<&Credential>,
) -> Result<Option<ClientFirst>> {
    let credential = match credential {
        Some(credential) => credential,
        None => return Ok(None),
    };

    // The spec indicates that SCRAM-SHA-256 should be assumed for speculative authentication if no
    // mechanism is provided. This doesn't cause issues with servers where SCRAM-SHA-256 is not the
    // default due to them being too old to support speculative authentication at all.
    let auth_mechanism = credential
        .mechanism
        .as_ref()
        .unwrap_or(&AuthMechanism::ScramSha256);

    let client_first = match auth_mechanism.build_speculative_client_first(credential)? {
        Some(client_first) => client_first,
        None => return Ok(None),
    };

    command.insert("speculativeAuthenticate", client_first.to_document());

    Ok(Some(client_first))
}
