#[cfg(test)]
mod test;

use std::time::Instant;

use lazy_static::lazy_static;
use os_info::{Type, Version};

use crate::{
    bson::{doc, Bson, Document},
    client::auth::{ClientFirst, FirstRound},
    cmap::{options::ConnectionPoolOptions, Command, Connection, StreamDescription},
    error::{ErrorKind, Result},
    is_master::{IsMasterCommandResponse, IsMasterReply},
    options::{AuthMechanism, ClientOptions, Credential, DriverInfo, ServerApi},
};

#[cfg(feature = "tokio-runtime")]
const RUNTIME_NAME: &str = "tokio";

#[cfg(all(feature = "async-std-runtime", not(feature = "sync")))]
const RUNTIME_NAME: &str = "async-std";

#[cfg(feature = "sync")]
const RUNTIME_NAME: &str = "sync (with async-std)";

#[derive(Clone, Debug)]
struct ClientMetadata {
    application: Option<AppMetadata>,
    driver: DriverMetadata,
    os: OsMetadata,
    platform: Option<String>,
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

        if let Some(platform) = metadata.platform {
            metadata_doc.insert("platform", platform);
        }

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
    /// the `isMaster` command.
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
            platform: None,
        };

        let info = os_info::get();

        if info.os_type() != Type::Unknown {
            let version = info.version();

            if *version != Version::Unknown {
                metadata.os.version = Some(info.version().to_string());
            }
        }

        if let Some((version, channel, date)) = version_check::triple() {
            metadata.platform =
                Some(format!("rustc {} {} ({}) with {}", version, channel, date, RUNTIME_NAME));
        }

        metadata
    };
}

/// Contains the logic needed to handshake a connection.
#[derive(Debug, Clone)]
pub(crate) struct Handshaker {
    /// The `isMaster` command to send when handshaking. This will always be identical
    /// given the same pool options, so it can be created at the time the Handshaker is created.
    command: Command,
    credential: Option<Credential>,
}

impl Handshaker {
    /// Creates a new Handshaker.
    pub(crate) fn new(options: Option<HandshakerOptions>) -> Self {
        let mut metadata = BASE_CLIENT_METADATA.clone();
        let mut credential = None;
        let mut db = None;
        let mut server_api = None;

        let mut body = doc! {
            "isMaster": 1,
        };

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

                if let Some(ref mut platform) = metadata.platform {
                    if let Some(ref driver_info_platform) = driver_info.platform {
                        platform.push('|');
                        platform.push_str(driver_info_platform);
                    }
                }
            }

            if let Some(cred) = options.credential {
                cred.append_needed_mechanism_negotiation(&mut body);
                db = Some(cred.resolved_source().to_string());
                credential = Some(cred);
            }

            server_api = options.server_api;
        }

        body.insert("client", metadata);

        let mut command = Command::new(
            "isMaster".to_string(),
            db.unwrap_or_else(|| "admin".to_string()),
            body,
        );

        if let Some(server_api) = server_api {
            command.set_server_api(&server_api)
        }

        Self {
            command,
            credential,
        }
    }

    /// Handshakes a connection.
    pub(crate) async fn handshake(&self, conn: &mut Connection) -> Result<HandshakeResult> {
        let mut command = self.command.clone();

        let client_first = set_speculative_auth_info(&mut command.body, self.credential.as_ref())?;

        let mut is_master_reply = is_master(command, conn).await?;
        conn.stream_description = Some(StreamDescription::from_is_master(is_master_reply.clone()));

        // Record the client's message and the server's response from speculative authentication if
        // the server did send a response.
        let first_round = client_first.and_then(|client_first| {
            is_master_reply
                .command_response
                .speculative_authenticate
                .take()
                .map(|server_first| client_first.into_first_round(server_first))
        });

        Ok(HandshakeResult {
            first_round,
            is_master_reply,
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
    pub(crate) is_master_reply: IsMasterReply,

    /// The first round of speculative authentication, if applicable.
    pub(crate) first_round: Option<FirstRound>,
}

#[derive(Debug)]
pub(crate) struct HandshakerOptions {
    app_name: Option<String>,
    credential: Option<Credential>,
    driver_info: Option<DriverInfo>,
    server_api: Option<ServerApi>,
}

impl From<ConnectionPoolOptions> for HandshakerOptions {
    fn from(options: ConnectionPoolOptions) -> Self {
        Self {
            app_name: options.app_name,
            credential: options.credential,
            driver_info: options.driver_info,
            server_api: options.server_api,
        }
    }
}

impl From<ClientOptions> for HandshakerOptions {
    fn from(options: ClientOptions) -> Self {
        Self {
            app_name: options.app_name,
            credential: options.credential,
            driver_info: options.driver_info,
            server_api: options.server_api,
        }
    }
}

/// Run the given isMaster command.
///
/// If the given command is not an isMaster, this function will return an error.
pub(crate) async fn is_master(command: Command, conn: &mut Connection) -> Result<IsMasterReply> {
    if !command.name.eq_ignore_ascii_case("ismaster") {
        return Err(ErrorKind::OperationError {
            message: format!("invalid ismaster command: {}", command.name),
        }
        .into());
    }
    let start_time = Instant::now();
    let response = conn.send_command(command, None).await?;
    let end_time = Instant::now();

    response.validate()?;
    let cluster_time = response.cluster_time().cloned();
    let command_response: IsMasterCommandResponse = response.body()?;

    Ok(IsMasterReply {
        command_response,
        round_trip_time: Some(end_time.duration_since(start_time)),
        cluster_time,
    })
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
