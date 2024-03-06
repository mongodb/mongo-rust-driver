#[cfg(test)]
mod test;

use std::env;

use once_cell::sync::Lazy;

use crate::{
    bson::{doc, Bson, Document},
    client::auth::ClientFirst,
    cmap::{Command, Connection, StreamDescription},
    compression::Compressor,
    error::Result,
    hello::{hello_command, run_hello, HelloReply},
    options::{AuthMechanism, Credential, DriverInfo, ServerApi},
};

#[cfg(not(feature = "sync"))]
const RUNTIME_NAME: &str = "tokio";

#[cfg(feature = "sync")]
const RUNTIME_NAME: &str = "sync (with tokio)";

#[derive(Clone, Debug)]
struct ClientMetadata {
    application: Option<AppMetadata>,
    driver: DriverMetadata,
    os: OsMetadata,
    platform: String,
    env: Option<RuntimeEnvironment>,
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
    architecture: Option<String>,
    version: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
struct RuntimeEnvironment {
    name: Option<FaasEnvironmentName>,
    runtime: Option<String>,
    timeout_sec: Option<i32>,
    memory_mb: Option<i32>,
    region: Option<String>,
    url: Option<String>,
    container: Option<Document>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum FaasEnvironmentName {
    AwsLambda,
    AzureFunc,
    GcpFunc,
    Vercel,
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

        if let Some(env) = metadata.env {
            metadata_doc.insert("env", env);
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

        if let Some(arch) = metadata.architecture {
            doc.insert("architecture", arch);
        }

        if let Some(version) = metadata.version {
            doc.insert("version", version);
        }

        Bson::Document(doc)
    }
}

impl From<RuntimeEnvironment> for Bson {
    fn from(env: RuntimeEnvironment) -> Self {
        let RuntimeEnvironment {
            name,
            runtime,
            timeout_sec,
            memory_mb,
            region,
            url,
            container,
        } = env;
        let mut out = doc! {};
        if let Some(name) = name {
            out.insert("name", name.name());
        }
        if let Some(rt) = runtime {
            out.insert("runtime", rt);
        }
        if let Some(t) = timeout_sec {
            out.insert("timeout_sec", t);
        }
        if let Some(m) = memory_mb {
            out.insert("memory_mb", m);
        }
        if let Some(r) = region {
            out.insert("region", r);
        }
        if let Some(u) = url {
            out.insert("url", u);
        }
        if let Some(c) = container {
            out.insert("container", c);
        }
        Bson::Document(out)
    }
}

impl RuntimeEnvironment {
    const UNSET: Self = RuntimeEnvironment {
        name: None,
        runtime: None,
        timeout_sec: None,
        memory_mb: None,
        region: None,
        url: None,
        container: None,
    };

    fn new() -> Option<Self> {
        let mut out = Self::UNSET;
        if let Some(name) = FaasEnvironmentName::new() {
            out.name = Some(name);
            match name {
                FaasEnvironmentName::AwsLambda => {
                    out.runtime = env::var("AWS_EXECUTION_ENV").ok();
                    out.region = env::var("AWS_REGION").ok();
                    out.memory_mb = env::var("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")
                        .ok()
                        .and_then(|s| s.parse().ok());
                }
                FaasEnvironmentName::AzureFunc => {
                    out.runtime = env::var("FUNCTIONS_WORKER_RUNTIME").ok();
                }
                FaasEnvironmentName::GcpFunc => {
                    out.memory_mb = env::var("FUNCTION_MEMORY_MB")
                        .ok()
                        .and_then(|s| s.parse().ok());
                    out.timeout_sec = env::var("FUNCTION_TIMEOUT_SEC")
                        .ok()
                        .and_then(|s| s.parse().ok());
                    out.region = env::var("FUNCTION_REGION").ok();
                }
                FaasEnvironmentName::Vercel => {
                    out.region = env::var("VERCEL_REGION").ok();
                }
            }
        }
        let mut container = doc! {};
        if std::path::Path::new("/.dockerenv").exists() {
            container.insert("runtime", "docker");
        }
        if var_set("KUBERNETES_SERVICE_HOST") {
            container.insert("orchestrator", "kubernetes");
        }
        if !container.is_empty() {
            out.container = Some(container);
        }
        if out == Self::UNSET {
            None
        } else {
            Some(out)
        }
    }
}

fn var_set(name: &str) -> bool {
    env::var_os(name).map_or(false, |v| !v.is_empty())
}

impl FaasEnvironmentName {
    pub(crate) fn new() -> Option<Self> {
        use FaasEnvironmentName::*;
        let mut found: Option<Self> = None;
        let lambda_env = env::var_os("AWS_EXECUTION_ENV")
            .map_or(false, |v| v.to_string_lossy().starts_with("AWS_Lambda_"));
        if lambda_env || var_set("AWS_LAMBDA_RUNTIME_API") {
            found = Some(AwsLambda);
        }
        if var_set("VERCEL") {
            // Vercel takes precedence over AwsLambda.
            found = Some(Vercel);
        }
        // Any other conflict is treated as unset.
        if var_set("FUNCTIONS_WORKER_RUNTIME") {
            match found {
                None => found = Some(AzureFunc),
                _ => return None,
            }
        }
        if var_set("K_SERVICE") || var_set("FUNCTION_NAME") {
            match found {
                None => found = Some(GcpFunc),
                _ => return None,
            }
        }
        found
    }

    fn name(&self) -> &'static str {
        use FaasEnvironmentName::*;
        match self {
            AwsLambda => "aws.lambda",
            AzureFunc => "azure.func",
            GcpFunc => "gcp.func",
            Vercel => "vercel",
        }
    }
}

/// Contains the basic handshake information that can be statically determined. This document
/// (potentially with additional fields added) can be cloned and put in the `client` field of
/// the `hello` or legacy hello command.
static BASE_CLIENT_METADATA: Lazy<ClientMetadata> = Lazy::new(|| ClientMetadata {
    application: None,
    driver: DriverMetadata {
        name: "mongo-rust-driver".into(),
        version: env!("CARGO_PKG_VERSION").into(),
    },
    os: OsMetadata {
        os_type: std::env::consts::OS.into(),
        architecture: Some(std::env::consts::ARCH.into()),
        name: None,
        version: None,
    },
    platform: format!(
        "{} with {}",
        rustc_version_runtime::version_meta().short_version_string,
        RUNTIME_NAME
    ),
    env: None,
});

type Truncation = fn(&mut ClientMetadata);

const METADATA_TRUNCATIONS: &[Truncation] = &[
    // clear `env.*` except `name`
    |metadata| {
        if let Some(env) = &mut metadata.env {
            *env = RuntimeEnvironment {
                name: env.name,
                ..RuntimeEnvironment::UNSET
            }
        }
    },
    // clear `os.*` except `type`
    |metadata| {
        metadata.os = OsMetadata {
            os_type: metadata.os.os_type.clone(),
            architecture: None,
            name: None,
            version: None,
        }
    },
    // clear `env`
    |metadata| {
        metadata.env = None;
    },
    // truncate `platform`
    |metadata| {
        metadata.platform = rustc_version_runtime::version_meta().short_version_string;
    },
];

/// Contains the logic needed to handshake a connection.
#[derive(Clone, Debug)]
pub(crate) struct Handshaker {
    /// The hello or legacy hello command to send when handshaking. This will always be identical
    /// given the same pool options, so it can be created at the time the Handshaker is created.
    command: Command,

    // This field is not read without a compression feature flag turned on.
    #[allow(dead_code)]
    compressors: Option<Vec<Compressor>>,

    server_api: Option<ServerApi>,

    metadata: ClientMetadata,

    #[cfg(feature = "aws-auth")]
    http_client: crate::runtime::HttpClient,
}

impl Handshaker {
    /// Creates a new Handshaker.
    pub(crate) fn new(options: HandshakerOptions) -> Self {
        let mut metadata = BASE_CLIENT_METADATA.clone();
        let compressors = options.compressors;

        let mut command = hello_command(
            options.server_api.as_ref(),
            options.load_balanced.into(),
            None,
            None,
        );

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

        metadata.env = RuntimeEnvironment::new();

        if options.load_balanced {
            command.body.insert("loadBalanced", true);
        }

        // Add compressors to handshake.
        // See https://github.com/mongodb/specifications/blob/master/source/compression/OP_COMPRESSED.rst
        if let Some(ref compressors) = compressors {
            command.body.insert(
                "compression",
                compressors
                    .iter()
                    .map(|x| x.name())
                    .collect::<Vec<&'static str>>(),
            );
        }

        command.body.insert("client", metadata.clone());

        Self {
            command,
            compressors,
            server_api: options.server_api,
            metadata,
            #[cfg(feature = "aws-auth")]
            http_client: crate::runtime::HttpClient::default(),
        }
    }

    /// Handshakes a connection.
    pub(crate) async fn handshake(
        &self,
        conn: &mut Connection,
        credential: Option<&Credential>,
    ) -> Result<HelloReply> {
        let mut command = self.command.clone();

        if let Some(cred) = credential {
            cred.append_needed_mechanism_negotiation(&mut command.body);
            command.target_db = cred.resolved_source().to_string();
        }

        let client_first = set_speculative_auth_info(&mut command.body, credential)?;

        let body = &mut command.body;
        let mut trunc_meta = self.metadata.clone();
        for trunc_fn in METADATA_TRUNCATIONS {
            if doc_size(body)? <= MAX_HELLO_SIZE {
                break;
            }
            trunc_fn(&mut trunc_meta);
            body.insert("client", trunc_meta.clone());
        }

        let mut hello_reply = run_hello(conn, command).await?;

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

        conn.server_id = hello_reply.command_response.connection_id;

        if let Some(credential) = credential {
            credential
                .authenticate_stream(
                    conn,
                    self.server_api.as_ref(),
                    first_round,
                    #[cfg(feature = "aws-auth")]
                    &self.http_client,
                )
                .await?
        }

        Ok(hello_reply)
    }
}

#[derive(Debug)]
pub(crate) struct HandshakerOptions {
    /// The application name specified by the user. This is sent to the server as part of the
    /// handshake that each connection makes when it's created.
    pub(crate) app_name: Option<String>,

    /// The compressors that the Client is willing to use in the order they are specified
    /// in the configuration.  The Client sends this list of compressors to the server.
    /// The server responds with the intersection of its supported list of compressors.
    pub(crate) compressors: Option<Vec<Compressor>>,

    /// Extra information to append to the driver version in the metadata of the handshake with the
    /// server. This should be used by libraries wrapping the driver, e.g. ODMs.
    pub(crate) driver_info: Option<DriverInfo>,

    /// The declared API version.
    ///
    /// The default value is to have no declared API version
    pub(crate) server_api: Option<ServerApi>,

    /// Whether or not the client is connecting to a MongoDB cluster through a load balancer.
    pub(crate) load_balanced: bool,
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

fn doc_size(d: &Document) -> Result<usize> {
    let mut tmp = vec![];
    d.to_writer(&mut tmp)?;
    Ok(tmp.len())
}

const MAX_HELLO_SIZE: usize = 512;
