#[cfg(test)]
mod test;

use std::env;

use crate::{
    bson::{rawdoc, RawBson, RawDocumentBuf},
    bson_compat::cstr,
    options::{AuthOptions, ClientOptions},
};
use std::sync::LazyLock;
use tokio::sync::broadcast;

#[cfg(any(
    feature = "zstd-compression",
    feature = "zlib-compression",
    feature = "snappy-compression"
))]
use crate::options::Compressor;
use crate::{
    client::auth::ClientFirst,
    cmap::{Command, Connection, StreamDescription},
    error::Result,
    hello::{hello_command, run_hello, HelloReply},
    options::{AuthMechanism, Credential, DriverInfo, ServerApi},
};

#[cfg(not(feature = "sync"))]
const RUNTIME_NAME: &str = "tokio";

#[cfg(feature = "sync")]
const RUNTIME_NAME: &str = "sync (with tokio)";

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClientMetadata {
    pub(crate) application: Option<AppMetadata>,
    pub(crate) driver: DriverMetadata,
    pub(crate) os: OsMetadata,
    pub(crate) platform: String,
    pub(crate) env: Option<RuntimeEnvironment>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct AppMetadata {
    pub(crate) name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DriverMetadata {
    pub(crate) name: String,
    pub(crate) version: String,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OsMetadata {
    pub(crate) os_type: String,
    pub(crate) name: Option<String>,
    pub(crate) architecture: Option<String>,
    pub(crate) version: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RuntimeEnvironment {
    pub(crate) name: Option<FaasEnvironmentName>,
    pub(crate) runtime: Option<String>,
    pub(crate) timeout_sec: Option<i32>,
    pub(crate) memory_mb: Option<i32>,
    pub(crate) region: Option<String>,
    pub(crate) url: Option<String>,
    pub(crate) container: Option<RawDocumentBuf>,
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub(crate) enum FaasEnvironmentName {
    AwsLambda,
    AzureFunc,
    GcpFunc,
    Vercel,
}

impl From<&ClientMetadata> for RawDocumentBuf {
    fn from(metadata: &ClientMetadata) -> Self {
        let mut metadata_doc = RawDocumentBuf::new();

        if let Some(application) = &metadata.application {
            metadata_doc.append(
                cstr!("application"),
                rawdoc! { "name": application.name.as_str() },
            );
        }

        metadata_doc.append(
            cstr!("driver"),
            rawdoc! {
                "name": metadata.driver.name.as_str(),
                "version": metadata.driver.version.as_str(),
            },
        );

        let raw_os: RawBson = (&metadata.os).into();
        metadata_doc.append(cstr!("os"), raw_os);
        metadata_doc.append(cstr!("platform"), metadata.platform.as_str());

        if let Some(env) = &metadata.env {
            let raw_env: RawBson = env.into();
            metadata_doc.append(cstr!("env"), raw_env);
        }

        metadata_doc
    }
}

impl From<&OsMetadata> for RawBson {
    fn from(metadata: &OsMetadata) -> Self {
        let mut doc = rawdoc! { "type": metadata.os_type.as_str() };

        if let Some(name) = &metadata.name {
            doc.append(cstr!("name"), name.as_str());
        }

        if let Some(arch) = &metadata.architecture {
            doc.append(cstr!("architecture"), arch.as_str());
        }

        if let Some(version) = &metadata.version {
            doc.append(cstr!("version"), version.as_str());
        }

        RawBson::Document(doc)
    }
}

impl From<&RuntimeEnvironment> for RawBson {
    fn from(env: &RuntimeEnvironment) -> Self {
        let RuntimeEnvironment {
            name,
            runtime,
            timeout_sec,
            memory_mb,
            region,
            url,
            container,
        } = env;
        let mut out = rawdoc! {};
        if let Some(name) = name {
            out.append(cstr!("name"), name.name());
        }
        if let Some(rt) = runtime {
            out.append(cstr!("runtime"), rt.as_str());
        }
        if let Some(t) = timeout_sec {
            out.append(cstr!("timeout_sec"), *t);
        }
        if let Some(m) = memory_mb {
            out.append(cstr!("memory_mb"), *m);
        }
        if let Some(r) = region {
            out.append(cstr!("region"), r.as_str());
        }
        if let Some(u) = url {
            out.append(cstr!("url"), u.as_str());
        }
        if let Some(c) = container {
            out.append(cstr!("container"), c.clone());
        }
        RawBson::Document(out)
    }
}

impl RuntimeEnvironment {
    pub(crate) const UNSET: Self = RuntimeEnvironment {
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
        let mut container = rawdoc! {};
        if std::path::Path::new("/.dockerenv").exists() {
            container.append(cstr!("runtime"), "docker");
        }
        if var_set("KUBERNETES_SERVICE_HOST") {
            container.append(cstr!("orchestrator"), "kubernetes");
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
    env::var_os(name).is_some_and(|v| !v.is_empty())
}

impl FaasEnvironmentName {
    pub(crate) fn new() -> Option<Self> {
        use FaasEnvironmentName::*;
        let mut found: Option<Self> = None;
        let lambda_env = env::var_os("AWS_EXECUTION_ENV")
            .is_some_and(|v| v.to_string_lossy().starts_with("AWS_Lambda_"));
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
pub(crate) static BASE_CLIENT_METADATA: LazyLock<ClientMetadata> =
    LazyLock::new(|| ClientMetadata {
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
            "{} with {} / bson-{}",
            rustc_version_runtime::version_meta().short_version_string,
            RUNTIME_NAME,
            if cfg!(feature = "bson-3") { "3" } else { "2" },
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

    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
    compressors: Option<Vec<Compressor>>,

    metadata: ClientMetadata,

    auth_options: AuthOptions,
}

#[cfg(test)]
#[allow(clippy::incompatible_msrv)]
pub(crate) static TEST_METADATA: std::sync::OnceLock<ClientMetadata> = std::sync::OnceLock::new();

impl Handshaker {
    /// Creates a new Handshaker.
    pub(crate) fn new(options: HandshakerOptions) -> Result<Self> {
        let mut metadata = BASE_CLIENT_METADATA.clone();

        let mut command = hello_command(
            options.server_api.as_ref(),
            options.load_balanced.into(),
            None,
            None,
        );

        if let Some(app_name) = options.app_name {
            metadata.application = Some(AppMetadata { name: app_name });
        }

        metadata.env = RuntimeEnvironment::new();

        if options.load_balanced {
            command.body.append(cstr!("loadBalanced"), true);
        }

        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        if let Some(ref compressors) = options.compressors {
            command.body.append(
                crate::bson_compat::cstr!("compression"),
                crate::bson::RawArrayBuf::from_iter(
                    compressors.iter().map(|compressor| compressor.name()),
                ),
            );
        }

        let mut handshaker = Self {
            command,
            #[cfg(any(
                feature = "zstd-compression",
                feature = "zlib-compression",
                feature = "snappy-compression"
            ))]
            compressors: options.compressors,
            metadata,
            auth_options: options.auth_options,
        };
        if let Some(driver_info) = options.driver_info {
            handshaker.append_metadata(driver_info);
        }

        Ok(handshaker)
    }

    pub(crate) fn append_metadata(&mut self, driver_info: DriverInfo) {
        self.metadata.driver.name.push('|');
        self.metadata.driver.name.push_str(&driver_info.name);

        if let Some(ref version) = driver_info.version {
            self.metadata.driver.version.push('|');
            self.metadata.driver.version.push_str(version);
        }

        if let Some(ref driver_info_platform) = driver_info.platform {
            self.metadata.platform.push('|');
            self.metadata.platform.push_str(driver_info_platform);
        }
    }

    async fn build_command(
        &self,
        credential: Option<&Credential>,
    ) -> Result<(Command, Option<ClientFirst>)> {
        let mut command = self.command.clone();
        command.target_db = "admin".to_string();

        if let Some(cred) = credential {
            cred.append_needed_mechanism_negotiation(&mut command.body);
        }

        let client_first = set_speculative_auth_info(&mut command.body, credential).await?;

        let body = &mut command.body;
        let body_size = body.as_bytes().len();
        let mut metadata = self.metadata.clone();
        let mut meta_doc: RawDocumentBuf = (&metadata).into();
        const OVERHEAD: usize = 1 /* tag */ + 6 /* name */ + 1 /* null */;
        for trunc_fn in METADATA_TRUNCATIONS {
            if body_size + OVERHEAD + meta_doc.as_bytes().len() <= MAX_HELLO_SIZE {
                break;
            }
            trunc_fn(&mut metadata);
            meta_doc = (&metadata).into();
        }
        #[cfg(test)]
        #[allow(clippy::incompatible_msrv)]
        let _ = TEST_METADATA.set(metadata);
        body.append(cstr!("client"), meta_doc);

        Ok((command, client_first))
    }

    /// Handshakes a connection.
    pub(crate) async fn handshake(
        &self,
        conn: &mut Connection,
        credential: Option<&Credential>,
        cancellation_receiver: Option<broadcast::Receiver<()>>,
    ) -> Result<HelloReply> {
        let (command, client_first) = self.build_command(credential).await?;
        let mut hello_reply = run_hello(conn, command, cancellation_receiver).await?;

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

        #[cfg(any(
            feature = "zstd-compression",
            feature = "zlib-compression",
            feature = "snappy-compression"
        ))]
        if let (Some(server_compressors), Some(client_compressors)) = (
            hello_reply.command_response.compressors.as_ref(),
            self.compressors.as_ref(),
        ) {
            // Use the first compressor in the user's list that is also supported by the server.
            if let Some(compressor) = client_compressors.iter().find(|client_compressor| {
                server_compressors
                    .iter()
                    .any(|server_compressor| client_compressor.name() == server_compressor)
            }) {
                conn.compressor = Some(compressor.clone());
            }
        }

        conn.server_id = hello_reply.command_response.connection_id;

        if let Some(credential) = credential {
            credential
                .authenticate_stream(conn, first_round, &self.auth_options)
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

    /// The compressors specified by the user. This list is sent to the server and the server
    /// replies with the subset of the compressors it supports.
    #[cfg(any(
        feature = "zstd-compression",
        feature = "zlib-compression",
        feature = "snappy-compression"
    ))]
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

    /// Auxiliary data for authentication mechanisms.
    pub(crate) auth_options: AuthOptions,
}

impl From<&ClientOptions> for HandshakerOptions {
    fn from(opts: &ClientOptions) -> Self {
        Self {
            app_name: opts.app_name.clone(),
            #[cfg(any(
                feature = "zstd-compression",
                feature = "zlib-compression",
                feature = "snappy-compression"
            ))]
            compressors: opts.compressors.clone(),
            driver_info: opts.driver_info.clone(),
            server_api: opts.server_api.clone(),
            load_balanced: opts.load_balanced.unwrap_or(false),
            auth_options: AuthOptions::from(opts),
        }
    }
}

/// Updates the handshake command document with the speculative authentication info.
async fn set_speculative_auth_info(
    command: &mut RawDocumentBuf,
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

    let client_first = match auth_mechanism
        .build_speculative_client_first(credential)
        .await?
    {
        Some(client_first) => client_first,
        None => return Ok(None),
    };

    command.append(
        cstr!("speculativeAuthenticate"),
        client_first.to_document()?,
    );

    Ok(Some(client_first))
}

const MAX_HELLO_SIZE: usize = 512;
