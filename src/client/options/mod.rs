#[cfg(all(test, not(feature = "sync")))]
mod test;

mod resolver_config;

use std::{
    collections::HashSet,
    convert::TryFrom,
    fmt::{self, Display, Formatter},
    fs::File,
    hash::{Hash, Hasher},
    io::{BufReader, Seek, SeekFrom},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use derivative::Derivative;
use lazy_static::lazy_static;
use rustls::{
    internal::pemfile,
    Certificate,
    RootCertStore,
    ServerCertVerified,
    ServerCertVerifier,
    TLSError,
};
use serde::{
    de::{Error, Unexpected},
    Deserialize,
    Deserializer,
    Serialize,
};
use serde_with::skip_serializing_none;
use strsim::jaro_winkler;
use typed_builder::TypedBuilder;
use webpki_roots::TLS_SERVER_ROOTS;

#[cfg(test)]
use crate::srv::LookupHosts;
use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    client::auth::{AuthMechanism, Credential},
    compression::Compressor,
    concern::{Acknowledgment, ReadConcern, WriteConcern},
    error::{ErrorKind, Result},
    event::{cmap::CmapEventHandler, command::CommandEventHandler, sdam::SdamEventHandler},
    options::ReadConcernLevel,
    sdam::MIN_HEARTBEAT_FREQUENCY,
    selection_criteria::{ReadPreference, SelectionCriteria, TagSet},
    srv::{OriginalSrvInfo, SrvResolver},
};

pub use resolver_config::ResolverConfig;

const DEFAULT_PORT: u16 = 27017;

const URI_OPTIONS: &[&str] = &[
    "appname",
    "authmechanism",
    "authsource",
    "authmechanismproperties",
    "compressors",
    "connecttimeoutms",
    "directconnection",
    "heartbeatfrequencyms",
    "journal",
    "localthresholdms",
    "maxidletimems",
    "maxstalenessseconds",
    "maxpoolsize",
    "minpoolsize",
    "readconcernlevel",
    "readpreference",
    "readpreferencetags",
    "replicaset",
    "retrywrites",
    "retryreads",
    "serverselectiontimeoutms",
    "sockettimeoutms",
    "tls",
    "ssl",
    "tlsinsecure",
    "tlsallowinvalidcertificates",
    "tlscafile",
    "tlscertificatekeyfile",
    "w",
    "waitqueuetimeoutms",
    "wtimeoutms",
    "zlibcompressionlevel",
];

lazy_static! {
    /// Reserved characters as defined by [Section 2.2 of RFC-3986](https://tools.ietf.org/html/rfc3986#section-2.2).
    /// Usernames / passwords that contain these characters must instead include the URL encoded version of them when included
    /// as part of the connection string.
    static ref USERINFO_RESERVED_CHARACTERS: HashSet<&'static char> = {
        [':', '/', '?', '#', '[', ']', '@'].iter().collect()
    };

    static ref ILLEGAL_DATABASE_CHARACTERS: HashSet<&'static char> = {
        ['/', '\\', ' ', '"', '$', '.'].iter().collect()
    };
}

/// An enum representing the address of a MongoDB server.
///
/// Currently this just supports addresses that can be connected to over TCP, but alternative
/// address types may be supported in the future (e.g. Unix Domain Socket paths).
#[derive(Clone, Debug, Eq, Serialize)]
#[non_exhaustive]
pub enum ServerAddress {
    /// A TCP/IP host and port combination.
    Tcp {
        /// The hostname or IP address where the MongoDB server can be found.
        host: String,

        /// The TCP port that the MongoDB server is listening on.
        ///
        /// The default is 27017.
        port: Option<u16>,
    },
}

impl<'de> Deserialize<'de> for ServerAddress {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Self::parse(s.as_str()).map_err(|e| D::Error::custom(format!("{}", e)))
    }
}

impl Default for ServerAddress {
    fn default() -> Self {
        Self::Tcp {
            host: "localhost".into(),
            port: None,
        }
    }
}

impl PartialEq for ServerAddress {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Tcp { host, port },
                Self::Tcp {
                    host: other_host,
                    port: other_port,
                },
            ) => host == other_host && port.unwrap_or(27017) == other_port.unwrap_or(27017),
        }
    }
}

impl Hash for ServerAddress {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        match self {
            Self::Tcp { host, port } => {
                host.hash(state);
                port.unwrap_or(27017).hash(state);
            }
        }
    }
}

impl FromStr for ServerAddress {
    type Err = crate::error::Error;
    fn from_str(address: &str) -> Result<Self> {
        ServerAddress::parse(address)
    }
}

impl ServerAddress {
    /// Parses an address string into a `ServerAddress`.
    pub fn parse(address: impl AsRef<str>) -> Result<Self> {
        let address = address.as_ref();
        let mut parts = address.split(':');
        let hostname = match parts.next() {
            Some(part) => {
                if part.is_empty() {
                    return Err(ErrorKind::InvalidArgument {
                        message: format!(
                            "invalid server address: \"{}\"; hostname cannot be empty",
                            address
                        ),
                    }
                    .into());
                }
                part
            }
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("invalid server address: \"{}\"", address),
                }
                .into())
            }
        };

        let port = match parts.next() {
            Some(part) => {
                let port = u16::from_str(part).map_err(|_| ErrorKind::InvalidArgument {
                    message: format!(
                        "port must be valid 16-bit unsigned integer, instead got: {}",
                        part
                    ),
                })?;

                if port == 0 {
                    return Err(ErrorKind::InvalidArgument {
                        message: format!(
                            "invalid server address: \"{}\"; port must be non-zero",
                            address
                        ),
                    }
                    .into());
                }
                if parts.next().is_some() {
                    return Err(ErrorKind::InvalidArgument {
                        message: format!(
                            "address \"{}\" contains more than one unescaped ':'",
                            address
                        ),
                    }
                    .into());
                }

                Some(port)
            }
            None => None,
        };

        Ok(ServerAddress::Tcp {
            host: hostname.to_lowercase(),
            port,
        })
    }

    #[cfg(all(test, not(feature = "sync")))]
    pub(crate) fn into_document(self) -> Document {
        match self {
            Self::Tcp { host, port } => {
                doc! {
                    "host": host,
                    "port": port.map(|i| Bson::Int32(i.into())).unwrap_or(Bson::Null)
                }
            }
        }
    }

    pub(crate) fn host(&self) -> &str {
        match self {
            Self::Tcp { host, .. } => host.as_str(),
        }
    }

    pub(crate) fn port(&self) -> Option<u16> {
        match self {
            Self::Tcp { port, .. } => *port,
        }
    }
}

impl fmt::Display for ServerAddress {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Tcp { host, port } => {
                write!(fmt, "{}:{}", host, port.unwrap_or(DEFAULT_PORT))
            }
        }
    }
}

/// Specifies the server API version to declare
#[derive(Clone, Debug, PartialEq, Serialize)]
#[non_exhaustive]
pub enum ServerApiVersion {
    /// Use API version 1.
    #[serde(rename = "1")]
    V1,
}

impl FromStr for ServerApiVersion {
    type Err = crate::error::Error;

    fn from_str(str: &str) -> Result<Self> {
        match str {
            "1" => Ok(Self::V1),
            _ => Err(ErrorKind::InvalidArgument {
                message: format!("invalid server api version string: {}", str),
            }
            .into()),
        }
    }
}

impl Display for ServerApiVersion {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::V1 => write!(f, "1"),
        }
    }
}

impl<'de> Deserialize<'de> for ServerApiVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;

        ServerApiVersion::from_str(&s)
            .map_err(|_| Error::invalid_value(Unexpected::Str(&s), &"a valid version number"))
    }
}

/// Options used to declare a versioned server API.  For more information, see the [Versioned API](
/// https://docs.mongodb.com/v5.0/reference/versioned-api/) manual page.
#[serde_with::skip_serializing_none]
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct ServerApi {
    /// The declared API version.
    #[serde(rename = "apiVersion")]
    pub version: ServerApiVersion,

    /// Whether the MongoDB server should reject all commands that are not part of the
    /// declared API version. This includes command options and aggregation pipeline stages.
    #[builder(default)]
    #[serde(rename = "apiStrict")]
    pub strict: Option<bool>,

    /// Whether the MongoDB server should return command failures when functionality that is
    /// deprecated from the declared API version is used.
    /// Note that at the time of this writing, no deprecations in version 1 exist.
    #[builder(default)]
    #[serde(rename = "apiDeprecationErrors")]
    pub deprecation_errors: Option<bool>,
}

/// Contains the options that can be used to create a new [`Client`](../struct.Client.html).
#[derive(Clone, Derivative, Deserialize, TypedBuilder)]
#[builder(field_defaults(setter(into)))]
#[derivative(Debug, PartialEq)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ClientOptions {
    /// The initial list of seeds that the Client should connect to.
    ///
    /// Note that by default, the driver will autodiscover other nodes in the cluster. To connect
    /// directly to a single server (rather than autodiscovering the rest of the cluster), set the
    /// `direct_connection` field to `true`.
    #[builder(default_code = "vec![ServerAddress::Tcp {
        host: \"localhost\".to_string(),
        port: Some(27017),
    }]")]
    #[serde(default = "default_hosts")]
    pub hosts: Vec<ServerAddress>,

    /// The application name that the Client will send to the server as part of the handshake. This
    /// can be used in combination with the server logs to determine which Client is connected to a
    /// server.
    #[builder(default)]
    pub app_name: Option<String>,

    /// The compressors that the Client is willing to use in the order they are specified
    /// in the configuration.  The Client sends this list of compressors to the server.
    /// The server responds with the intersection of its supported list of compressors.
    /// The order of compressors indicates preference of compressors.
    #[builder(default)]
    #[serde(skip)]
    pub compressors: Option<Vec<Compressor>>,

    /// The handler that should process all Connection Monitoring and Pooling events. See the
    /// CmapEventHandler type documentation for more details.
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    #[builder(default)]
    #[serde(skip)]
    pub cmap_event_handler: Option<Arc<dyn CmapEventHandler>>,

    /// The handler that should process all command-related events. See the CommandEventHandler
    /// type documentation for more details.
    ///
    /// Note that monitoring command events may incur a performance penalty.
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    #[builder(default)]
    #[serde(skip)]
    pub command_event_handler: Option<Arc<dyn CommandEventHandler>>,

    /// The connect timeout passed to each underlying TcpStream when attemtping to connect to the
    /// server.
    ///
    /// The default value is 10 seconds.
    #[builder(default)]
    pub connect_timeout: Option<Duration>,

    /// The credential to use for authenticating connections made by this client.
    #[builder(default)]
    pub credential: Option<Credential>,

    /// Specifies whether the Client should directly connect to a single host rather than
    /// autodiscover all servers in the cluster.
    ///
    /// The default value is false.
    #[builder(default)]
    pub direct_connection: Option<bool>,

    /// Extra information to append to the driver version in the metadata of the handshake with the
    /// server. This should be used by libraries wrapping the driver, e.g. ODMs.
    #[builder(default)]
    pub driver_info: Option<DriverInfo>,

    /// The amount of time each monitoring thread should wait between sending an isMaster command
    /// to its respective server.
    ///
    /// The default value is 10 seconds.
    #[builder(default)]
    pub heartbeat_freq: Option<Duration>,

    /// When running a read operation with a ReadPreference that allows selecting secondaries,
    /// `local_threshold` is used to determine how much longer the average round trip time between
    /// the driver and server is allowed compared to the least round trip time of all the suitable
    /// servers. For example, if the average round trip times of the suitable servers are 5 ms, 10
    /// ms, and 15 ms, and the local threshold is 8 ms, then the first two servers are within the
    /// latency window and could be chosen for the operation, but the last one is not.
    ///
    /// A value of zero indicates that there is no latency window, so only the server with the
    /// lowest average round trip time is eligible.
    ///
    /// The default value is 15 ms.
    #[builder(default)]
    pub local_threshold: Option<Duration>,

    /// The amount of time that a connection can remain idle in a connection pool before being
    /// closed. A value of zero indicates that connections should not be closed due to being idle.
    ///
    /// By default, connections will not be closed due to being idle.
    #[builder(default)]
    pub max_idle_time: Option<Duration>,

    /// The maximum amount of connections that the Client should allow to be created in a
    /// connection pool for a given server. If an operation is attempted on a server while
    /// `max_pool_size` connections are checked out, the operation will block until an in-progress
    /// operation finishes and its connection is checked back into the pool.
    ///
    /// The default value is 100.
    #[builder(default)]
    pub max_pool_size: Option<u32>,

    /// The minimum number of connections that should be available in a server's connection pool at
    /// a given time. If fewer than `min_pool_size` connections are in the pool, connections will
    /// be added to the pool in the background until `min_pool_size` is reached.
    ///
    /// The default value is 0.
    #[builder(default)]
    pub min_pool_size: Option<u32>,

    /// Specifies the default read concern for operations performed on the Client. See the
    /// ReadConcern type documentation for more details.
    #[builder(default)]
    pub read_concern: Option<ReadConcern>,

    /// The name of the replica set that the Client should connect to.
    #[builder(default)]
    pub repl_set_name: Option<String>,

    /// Whether or not the client should retry a read operation if the operation fails.
    ///
    /// The default value is true.
    #[builder(default)]
    pub retry_reads: Option<bool>,

    /// Whether or not the client should retry a write operation if the operation fails.
    ///
    /// The default value is true.
    #[builder(default)]
    pub retry_writes: Option<bool>,

    /// The handler that should process all Server Discovery and Monitoring events. See the
    /// [`SdamEventHandler`] type documentation for more details.
    #[derivative(Debug = "ignore", PartialEq = "ignore")]
    #[builder(default)]
    #[serde(skip)]
    pub sdam_event_handler: Option<Arc<dyn SdamEventHandler>>,

    /// The default selection criteria for operations performed on the Client. See the
    /// SelectionCriteria type documentation for more details.
    #[builder(default)]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The declared API version for this client.
    /// The declared API version is applied to all commands run through the client, including those
    /// sent through any handle derived from the client.
    ///
    /// Specifying versioned API options in the command document passed to `run_command` AND
    /// declaring an API version on the client is not supported and is considered undefined
    /// behaviour. To run any command with a different API version or without declaring one, create
    /// a separate client that declares the appropriate API version.
    ///
    /// For more information, see the [Versioned API](
    /// https://docs.mongodb.com/v5.0/reference/versioned-api/) manual page.
    #[builder(default)]
    pub server_api: Option<ServerApi>,

    /// The amount of time the Client should attempt to select a server for an operation before
    /// timing outs
    ///
    /// The default value is 30 seconds.
    #[builder(default)]
    pub server_selection_timeout: Option<Duration>,

    #[builder(default, setter(skip))]
    pub(crate) socket_timeout: Option<Duration>,

    /// The TLS configuration for the Client to use in its connections with the server.
    ///
    /// By default, TLS is disabled.
    #[builder(default)]
    pub tls: Option<Tls>,

    /// Specifies the default write concern for operations performed on the Client. See the
    /// WriteConcern type documentation for more details.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// Information from the SRV URI that generated these client options, if applicable.
    #[builder(default, setter(skip))]
    #[serde(skip)]
    pub(crate) original_srv_info: Option<OriginalSrvInfo>,

    #[builder(default, setter(skip))]
    pub(crate) original_uri: Option<String>,

    /// Configuration of the trust-dns resolver used for SRV and TXT lookups.
    /// By default, the host system's resolver configuration will be used.
    ///
    /// On Windows, there is a known performance issue in trust-dns with using the default system
    /// configuration, so a custom configuration is recommended.
    #[builder(default, setter(skip))]
    #[serde(skip)]
    pub(crate) resolver_config: Option<ResolverConfig>,

    /// Whether or not the client is connecting to a MongoDB cluster through a load balancer.
    #[builder(default, setter(skip))]
    #[serde(rename = "loadbalanced")]
    pub(crate) load_balanced: Option<bool>,

    /// Control test behavior of the client.
    #[cfg(test)]
    #[builder(default, setter(skip))]
    #[serde(skip)]
    #[derivative(PartialEq = "ignore")]
    pub(crate) test_options: Option<TestOptions>,
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct TestOptions {
    /// Override MIN_HEARTBEAT_FREQUENCY.
    pub(crate) heartbeat_freq: Option<Duration>,

    /// Disable server and SRV-polling monitor threads.
    pub(crate) disable_monitoring_threads: bool,

    /// Mock response for `SrvPollingMonitor::lookup_hosts`.
    pub(crate) mock_lookup_hosts: Option<Result<LookupHosts>>,

    /// Mock the `serviceId` response for a load-balanced hello.
    pub(crate) mock_service_id: bool,
}

fn default_hosts() -> Vec<ServerAddress> {
    vec![ServerAddress::default()]
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
impl Serialize for ClientOptions {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct ClientOptionsHelper<'a> {
            appname: &'a Option<String>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            connecttimeoutms: &'a Option<Duration>,

            #[serde(flatten, serialize_with = "Credential::serialize_for_client_options")]
            credential: &'a Option<Credential>,

            directconnection: &'a Option<bool>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            heartbeatfrequencyms: &'a Option<Duration>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            localthresholdms: &'a Option<Duration>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            maxidletimems: &'a Option<Duration>,

            maxpoolsize: &'a Option<u32>,

            minpoolsize: &'a Option<u32>,

            #[serde(flatten, serialize_with = "ReadConcern::serialize_for_client_options")]
            readconcern: &'a Option<ReadConcern>,

            replicaset: &'a Option<String>,

            retryreads: &'a Option<bool>,

            retrywrites: &'a Option<bool>,

            #[serde(
                flatten,
                serialize_with = "SelectionCriteria::serialize_for_client_options"
            )]
            selectioncriteria: &'a Option<SelectionCriteria>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            serverselectiontimeoutms: &'a Option<Duration>,

            #[serde(serialize_with = "bson_util::serialize_duration_option_as_int_millis")]
            sockettimeoutms: &'a Option<Duration>,

            #[serde(flatten, serialize_with = "Tls::serialize_for_client_options")]
            tls: &'a Option<Tls>,

            #[serde(flatten, serialize_with = "WriteConcern::serialize_for_client_options")]
            writeconcern: &'a Option<WriteConcern>,

            zlibcompressionlevel: &'a Option<i32>,

            loadbalanced: &'a Option<bool>,
        }

        let client_options = ClientOptionsHelper {
            appname: &self.app_name,
            connecttimeoutms: &self.connect_timeout,
            credential: &self.credential,
            directconnection: &self.direct_connection,
            heartbeatfrequencyms: &self.heartbeat_freq,
            localthresholdms: &self.local_threshold,
            maxidletimems: &self.max_idle_time,
            maxpoolsize: &self.max_pool_size,
            minpoolsize: &self.min_pool_size,
            readconcern: &self.read_concern,
            replicaset: &self.repl_set_name,
            retryreads: &self.retry_reads,
            retrywrites: &self.retry_writes,
            selectioncriteria: &self.selection_criteria,
            serverselectiontimeoutms: &self.server_selection_timeout,
            sockettimeoutms: &self.socket_timeout,
            tls: &self.tls,
            writeconcern: &self.write_concern,
            loadbalanced: &self.load_balanced,
            zlibcompressionlevel: &None,
        };

        client_options.serialize(serializer)
    }
}

#[derive(Debug, Default, PartialEq)]
struct ClientOptionsParser {
    pub hosts: Vec<ServerAddress>,
    pub srv: bool,
    pub app_name: Option<String>,
    pub tls: Option<Tls>,
    pub heartbeat_freq: Option<Duration>,
    pub local_threshold: Option<Duration>,
    pub read_concern: Option<ReadConcern>,
    pub selection_criteria: Option<SelectionCriteria>,
    pub repl_set_name: Option<String>,
    pub write_concern: Option<WriteConcern>,
    pub server_selection_timeout: Option<Duration>,
    pub max_pool_size: Option<u32>,
    pub min_pool_size: Option<u32>,
    pub max_idle_time: Option<Duration>,
    pub wait_queue_timeout: Option<Duration>,
    pub compressors: Option<Vec<Compressor>>,
    pub connect_timeout: Option<Duration>,
    pub retry_reads: Option<bool>,
    pub retry_writes: Option<bool>,
    pub socket_timeout: Option<Duration>,
    pub zlib_compression: Option<i32>,
    pub direct_connection: Option<bool>,
    pub credential: Option<Credential>,
    max_staleness: Option<Duration>,
    tls_insecure: Option<bool>,
    auth_mechanism: Option<AuthMechanism>,
    auth_source: Option<String>,
    auth_mechanism_properties: Option<Document>,
    read_preference: Option<ReadPreference>,
    read_preference_tags: Option<Vec<TagSet>>,
    load_balanced: Option<bool>,
    original_uri: String,
}

/// Specifies whether TLS configuration should be used with the operations that the
/// [`Client`](../struct.Client.html) performs.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub enum Tls {
    /// Enable TLS with the specified options.
    Enabled(TlsOptions),

    /// Disable TLS.
    Disabled,
}

impl From<TlsOptions> for Tls {
    fn from(options: TlsOptions) -> Self {
        Self::Enabled(options)
    }
}

impl From<TlsOptions> for Option<Tls> {
    fn from(options: TlsOptions) -> Self {
        Some(Tls::Enabled(options))
    }
}

impl Tls {
    #[cfg(test)]
    pub(crate) fn serialize_for_client_options<S>(
        tls: &Option<Tls>,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match tls {
            Some(Tls::Enabled(tls_options)) => {
                TlsOptions::serialize_for_client_options(tls_options, serializer)
            }
            _ => serializer.serialize_none(),
        }
    }
}

/// Specifies the TLS configuration that the [`Client`](../struct.Client.html) should use.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[non_exhaustive]
pub struct TlsOptions {
    /// Whether or not the [`Client`](../struct.Client.html) should return an error if the server
    /// presents an invalid certificate. This setting should _not_ be set to `true` in
    /// production; it should only be used for testing.
    ///
    /// The default value is to error when the server presents an invalid certificate.
    pub allow_invalid_certificates: Option<bool>,

    /// The path to the CA file that the [`Client`](../struct.Client.html) should use for TLS. If
    /// none is specified, then the driver will use the Mozilla root certificates from the
    /// `webpki-roots` crate.
    pub ca_file_path: Option<PathBuf>,

    /// The path to the certificate file that the [`Client`](../struct.Client.html) should present
    /// to the server to verify its identify. If none is specified, then the
    /// [`Client`](../struct.Client.html) will not attempt to verify its identity to the
    /// server.
    pub cert_key_file_path: Option<PathBuf>,
}

struct NoCertVerifier {}

impl ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _: &RootCertStore,
        _: &[Certificate],
        _: webpki::DNSNameRef,
        _: &[u8],
    ) -> std::result::Result<ServerCertVerified, TLSError> {
        Ok(ServerCertVerified::assertion())
    }
}

impl TlsOptions {
    /// Converts `TlsOptions` into a rustls::ClientConfig.
    pub(crate) fn into_rustls_config(self) -> Result<rustls::ClientConfig> {
        let mut config = rustls::ClientConfig::new();

        if let Some(true) = self.allow_invalid_certificates {
            config
                .dangerous()
                .set_certificate_verifier(Arc::new(NoCertVerifier {}));
        }

        let mut store = RootCertStore::empty();
        if let Some(path) = self.ca_file_path {
            store
                .add_pem_file(&mut BufReader::new(File::open(&path)?))
                .map_err(|_| ErrorKind::InvalidTlsConfig {
                    message: format!(
                        "Unable to parse PEM-encoded root certificate from {}",
                        path.display()
                    ),
                })?;
        } else {
            store.add_server_trust_anchors(&TLS_SERVER_ROOTS);
        }

        config.root_store = store;

        if let Some(path) = self.cert_key_file_path {
            let mut file = BufReader::new(File::open(&path)?);
            let certs = match pemfile::certs(&mut file) {
                Ok(certs) => certs,
                Err(()) => {
                    return Err(ErrorKind::InvalidTlsConfig {
                        message: format!(
                            "Unable to parse PEM-encoded client certificate from {}",
                            path.display()
                        ),
                    }
                    .into())
                }
            };

            file.seek(SeekFrom::Start(0))?;
            let key = match pemfile::rsa_private_keys(&mut file) {
                Ok(key) => key,
                Err(()) => {
                    return Err(ErrorKind::InvalidTlsConfig {
                        message: format!(
                            "Unable to parse PEM-encoded RSA key from {}",
                            path.display()
                        ),
                    }
                    .into())
                }
            };

            // TODO: Get rid of unwrap.
            config
                .set_single_client_cert(certs, key.into_iter().next().unwrap())
                .map_err(|e| ErrorKind::InvalidTlsConfig {
                    message: e.to_string(),
                })?;
        }

        Ok(config)
    }

    #[cfg(test)]
    pub(crate) fn serialize_for_client_options<S>(
        tls_options: &TlsOptions,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        #[derive(Serialize)]
        struct TlsOptionsHelper<'a> {
            tls: bool,
            tlscafile: Option<&'a str>,
            tlscertificatekeyfile: Option<&'a str>,
            tlsallowinvalidcertificates: Option<bool>,
        }

        let state = TlsOptionsHelper {
            tls: true,
            tlscafile: tls_options
                .ca_file_path
                .as_ref()
                .map(|s| s.to_str().unwrap()),
            tlscertificatekeyfile: tls_options
                .cert_key_file_path
                .as_ref()
                .map(|s| s.to_str().unwrap()),
            tlsallowinvalidcertificates: tls_options.allow_invalid_certificates,
        };
        state.serialize(serializer)
    }
}

/// Extra information to append to the driver version in the metadata of the handshake with the
/// server. This should be used by libraries wrapping the driver, e.g. ODMs.
#[derive(Clone, Debug, Deserialize, TypedBuilder, PartialEq)]
#[builder(field_defaults(setter(into)))]
#[non_exhaustive]
pub struct DriverInfo {
    /// The name of the library wrapping the driver.
    pub name: String,

    /// The version of the library wrapping the driver.
    #[builder(default)]
    pub version: Option<String>,

    /// Optional platform information for the wrapping driver.
    #[builder(default)]
    pub platform: Option<String>,
}

impl From<ClientOptionsParser> for ClientOptions {
    fn from(parser: ClientOptionsParser) -> Self {
        Self {
            hosts: parser.hosts,
            app_name: parser.app_name,
            tls: parser.tls,
            heartbeat_freq: parser.heartbeat_freq,
            local_threshold: parser.local_threshold,
            read_concern: parser.read_concern,
            selection_criteria: parser.selection_criteria,
            repl_set_name: parser.repl_set_name,
            write_concern: parser.write_concern,
            max_pool_size: parser.max_pool_size,
            min_pool_size: parser.min_pool_size,
            max_idle_time: parser.max_idle_time,
            server_selection_timeout: parser.server_selection_timeout,
            compressors: parser.compressors,
            connect_timeout: parser.connect_timeout,
            retry_reads: parser.retry_reads,
            retry_writes: parser.retry_writes,
            socket_timeout: parser.socket_timeout,
            direct_connection: parser.direct_connection,
            driver_info: None,
            credential: parser.credential,
            cmap_event_handler: None,
            command_event_handler: None,
            original_srv_info: None,
            original_uri: Some(parser.original_uri),
            resolver_config: None,
            server_api: None,
            load_balanced: parser.load_balanced,
            sdam_event_handler: None,
            #[cfg(test)]
            test_options: None,
        }
    }
}

impl ClientOptions {
    /// Creates a new ClientOptions with the `original_srv_hostname` field set to the testing value
    /// used in the SRV tests.
    #[cfg(test)]
    pub(crate) fn new_srv() -> Self {
        Self {
            original_srv_info: Some(OriginalSrvInfo {
                hostname: "localhost.test.test.build.10gen.cc".into(),
                min_ttl: Duration::from_secs(60),
            }),
            ..Default::default()
        }
    }

    /// Parses a MongoDB connection string into a [`ClientOptions`] struct. If the string is
    /// malformed or one of the options has an invalid value, an error will be returned.
    ///
    /// In the case that "mongodb+srv" is used, SRV and TXT record lookups will be done as
    /// part of this method.
    ///
    /// The format of a MongoDB connection string is described [here](https://docs.mongodb.com/manual/reference/connection-string/#connection-string-formats).
    ///
    /// The following options are supported in the options query string:
    ///
    ///   * `appName`: maps to the `app_name` field
    ///   * `authMechanism`: maps to the `mechanism` field of the `credential` field
    ///   * `authSource`: maps to the `source` field of the `credential` field
    ///   * `authMechanismProperties`: maps to the `mechanism_properties` field of the `credential`
    ///     field
    ///   * `compressors`: maps to the `compressors` field
    ///   * `connectTimeoutMS`: maps to the `connect_timeout` field
    ///   * `direct`: maps to the `direct` field
    ///   * `heartbeatFrequencyMS`: maps to the `heartbeat_frequency` field
    ///   * `journal`: maps to the `journal` field of the `write_concern` field
    ///   * `localThresholdMS`: maps to the `local_threshold` field
    ///   * `maxIdleTimeMS`: maps to the `max_idle_time` field
    ///   * `maxStalenessSeconds`: maps to the `max_staleness` field of the `selection_criteria`
    ///     field
    ///   * `maxPoolSize`: maps to the `max_pool_size` field
    ///   * `minPoolSize`: maps to the `min_pool_size` field
    ///   * `readConcernLevel`: maps to the `read_concern` field
    ///   * `readPreferenceField`: maps to the ReadPreference enum variant of the
    ///     `selection_criteria` field
    ///   * `readPreferenceTags`: maps to the `tags` field of the `selection_criteria` field. Note
    ///     that this option can appear more than once; each instance will be mapped to a separate
    ///     tag set
    ///   * `replicaSet`: maps to the `repl_set_name` field
    ///   * `retryWrites`: not yet implemented
    ///   * `retryReads`: maps to the `retry_reads` field
    ///   * `serverSelectionTimeoutMS`: maps to the `server_selection_timeout` field
    ///   * `socketTimeoutMS`: unsupported, does not map to any field
    ///   * `ssl`: an alias of the `tls` option
    ///   * `tls`: maps to the TLS variant of the `tls` field`.
    ///   * `tlsInsecure`: relaxes the TLS constraints on connections being made; currently is just
    ///     an alias of `tlsAllowInvalidCertificates`, but more behavior may be added to this option
    ///     in the future
    ///   * `tlsAllowInvalidCertificates`: maps to the `allow_invalidCertificates` field of the
    ///     `tls` field
    ///   * `tlsCAFile`: maps to the `ca_file_path` field of the `tls` field
    ///   * `tlsCertificateKeyFile`: maps to the `cert_key_file_path` field of the `tls` field
    ///   * `w`: maps to the `w` field of the `write_concern` field
    ///   * `waitQueueTimeoutMS`: unsupported, does not map to any field
    ///   * `wTimeoutMS`: maps to the `w_timeout` field of the `write_concern` field
    ///   * `zlibCompressionLevel`: maps to the `level` field of the `Compressor::Zlib` variant
    ///     (which requires the `zlib-compression` feature flag) of the [`Compressor`] enum
    ///
    /// Note: if the `sync` feature is enabled, then this method will be replaced with [the sync
    /// version](#method.parse-1).
    #[cfg(not(feature = "sync"))]
    pub async fn parse(s: impl AsRef<str>) -> Result<Self> {
        Self::parse_uri(s, None).await
    }

    /// This method will be present if the `sync` feature is enabled. It's otherwise identical to
    /// [the async version](#method.parse)
    #[cfg(any(feature = "sync", docsrs))]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    pub fn parse(s: impl AsRef<str>) -> Result<Self> {
        crate::RUNTIME.block_on(Self::parse_uri(s.as_ref(), None))
    }

    /// Parses a MongoDB connection string into a `ClientOptions` struct.
    /// If the string is malformed or one of the options has an invalid value, an error will be
    /// returned.
    ///
    /// In the case that "mongodb+srv" is used, SRV and TXT record lookups will be done using the
    /// provided `ResolverConfig` as part of this method.
    ///
    /// The format of a MongoDB connection string is described [here](https://docs.mongodb.com/manual/reference/connection-string/#connection-string-formats).
    ///
    /// See the docstring on `ClientOptions::parse` for information on how the various URI options
    /// map to fields on `ClientOptions`.
    ///
    /// Note: if the `sync` feature is enabled, then this method will be replaced with [the sync
    /// version](#method.parse_with_resolver_config-1).
    #[cfg(not(feature = "sync"))]
    pub async fn parse_with_resolver_config(
        uri: impl AsRef<str>,
        resolver_config: ResolverConfig,
    ) -> Result<Self> {
        Self::parse_uri(uri, Some(resolver_config)).await
    }

    /// This method will be present if the `sync` feature is enabled. It's otherwise identical to
    /// [the async version](#method.parse_with_resolver_config)
    #[cfg(any(feature = "sync", docsrs))]
    #[cfg_attr(docsrs, doc(cfg(feature = "sync")))]
    pub fn parse_with_resolver_config(uri: &str, resolver_config: ResolverConfig) -> Result<Self> {
        crate::RUNTIME.block_on(Self::parse_uri(uri, Some(resolver_config)))
    }

    /// Populate this `ClientOptions` from the given URI, optionally using the resolver config for
    /// DNS lookups.
    pub(crate) async fn parse_uri(
        uri: impl AsRef<str>,
        resolver_config: Option<ResolverConfig>,
    ) -> Result<Self> {
        let parser = ClientOptionsParser::parse(uri.as_ref())?;
        let srv = parser.srv;
        let auth_source_present = parser.auth_source.is_some();
        let mut options: Self = parser.into();
        options.resolver_config = resolver_config.clone();

        if srv {
            let mut resolver = SrvResolver::new(resolver_config.map(|config| config.inner)).await?;
            let mut config = resolver
                .resolve_client_options(options.hosts[0].host())
                .await?;

            // Save the original SRV info to allow mongos polling.
            options.original_srv_info = OriginalSrvInfo {
                hostname: options.hosts[0].host().to_string(),
                min_ttl: config.min_ttl,
            }
            .into();

            // Set the ClientOptions hosts to those found during the SRV lookup.
            options.hosts = config.hosts;

            // Enable TLS unless the user explicitly disabled it.
            if options.tls.is_none() {
                options.tls = Some(Tls::Enabled(Default::default()));
            }

            // Set the authSource TXT option found during SRV lookup unless the user already set it.
            // Note that this _does_ override the default database specified in the URI, since it is
            // supposed to be overriden by authSource.
            if !auth_source_present {
                if let Some(auth_source) = config.auth_source.take() {
                    if let Some(ref mut credential) = options.credential {
                        credential.source = Some(auth_source);
                    }
                }
            }

            // Set the replica set name TXT option found during SRV lookup unless the user already
            // set it.
            if options.repl_set_name.is_none() {
                if let Some(replica_set) = config.replica_set.take() {
                    options.repl_set_name = Some(replica_set);
                }
            }

            if options.load_balanced.is_none() {
                options.load_balanced = config.load_balanced;
            }
        }

        options.validate()?;
        Ok(options)
    }

    #[cfg(test)]
    pub(crate) fn parse_without_srv_resolution(s: &str) -> Result<Self> {
        let parser = ClientOptionsParser::parse(s)?;
        let options: Self = parser.into();
        options.validate()?;

        Ok(options)
    }

    pub(crate) fn tls_options(&self) -> Option<TlsOptions> {
        match self.tls {
            Some(Tls::Enabled(ref opts)) => Some(opts.clone()),
            _ => None,
        }
    }

    /// Ensure the options set are valid, returning an error describing the problem if they are not.
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(true) = self.direct_connection {
            if self.hosts.len() > 1 {
                return Err(ErrorKind::InvalidArgument {
                    message: "cannot specify multiple seeds with directConnection=true".to_string(),
                }
                .into());
            }
        }

        if let Some(ref write_concern) = self.write_concern {
            write_concern.validate()?;
        }

        if self.load_balanced.unwrap_or(false) {
            if self.hosts.len() > 1 {
                return Err(ErrorKind::InvalidArgument {
                    message: "cannot specify multiple seeds with loadBalanced=true".to_string(),
                }
                .into());
            }
            if self.repl_set_name.is_some() {
                return Err(ErrorKind::InvalidArgument {
                    message: "cannot specify replicaSet with loadBalanced=true".to_string(),
                }
                .into());
            }
            if self.direct_connection == Some(true) {
                return Err(ErrorKind::InvalidArgument {
                    message: "cannot specify directConnection=true with loadBalanced=true"
                        .to_string(),
                }
                .into());
            }
        }

        if let Some(ref compressors) = self.compressors {
            for compressor in compressors {
                compressor.validate()?;
            }
        }

        Ok(())
    }

    /// Applies the options in other to these options if a value is not already present
    #[cfg(test)]
    pub(crate) fn merge(&mut self, other: ClientOptions) {
        merge_options!(
            other,
            self,
            [
                app_name,
                compressors,
                cmap_event_handler,
                command_event_handler,
                connect_timeout,
                credential,
                direct_connection,
                driver_info,
                heartbeat_freq,
                local_threshold,
                max_idle_time,
                max_pool_size,
                min_pool_size,
                read_concern,
                repl_set_name,
                retry_reads,
                retry_writes,
                selection_criteria,
                server_api,
                server_selection_timeout,
                socket_timeout,
                tls,
                write_concern,
                original_srv_info,
                original_uri
            ]
        );
    }

    #[cfg(test)]
    pub(crate) fn test_options_mut(&mut self) -> &mut TestOptions {
        self.test_options.get_or_insert_with(Default::default)
    }
}

/// Splits a string into a section before a given index and a section exclusively after the index.
/// Empty portions are returned as `None`.
fn exclusive_split_at(s: &str, i: usize) -> (Option<&str>, Option<&str>) {
    let (l, r) = s.split_at(i);

    let lout = if !l.is_empty() { Some(l) } else { None };
    let rout = if r.len() > 1 { Some(&r[1..]) } else { None };

    (lout, rout)
}

fn percent_decode(s: &str, err_message: &str) -> Result<String> {
    match percent_encoding::percent_decode_str(s).decode_utf8() {
        Ok(result) => Ok(result.to_string()),
        Err(_) => Err(ErrorKind::InvalidArgument {
            message: err_message.to_string(),
        }
        .into()),
    }
}

fn validate_userinfo(s: &str, userinfo_type: &str) -> Result<()> {
    if s.chars().any(|c| USERINFO_RESERVED_CHARACTERS.contains(&c)) {
        return Err(ErrorKind::InvalidArgument {
            message: format!("{} must be URL encoded", userinfo_type),
        }
        .into());
    }

    // All instances of '%' in the username must be part of an percent-encoded substring. This means
    // that there must be two hexidecimal digits following any '%' in the username.
    if s.split('%')
        .skip(1)
        .any(|part| part.len() < 2 || part[0..2].chars().any(|c| !c.is_ascii_hexdigit()))
    {
        return Err(ErrorKind::InvalidArgument {
            message: "username/password cannot contain unescaped %".to_string(),
        }
        .into());
    }

    Ok(())
}

impl ClientOptionsParser {
    fn parse(s: &str) -> Result<Self> {
        let end_of_scheme = match s.find("://") {
            Some(index) => index,
            None => {
                return Err(ErrorKind::InvalidArgument {
                    message: "connection string contains no scheme".to_string(),
                }
                .into())
            }
        };

        let srv = match &s[..end_of_scheme] {
            "mongodb" => false,
            "mongodb+srv" => true,
            _ => {
                return Err(ErrorKind::InvalidArgument {
                    message: format!("invalid connection string scheme: {}", &s[..end_of_scheme]),
                }
                .into())
            }
        };

        let after_scheme = &s[end_of_scheme + 3..];

        let (pre_slash, post_slash) = match after_scheme.find('/') {
            Some(slash_index) => match exclusive_split_at(after_scheme, slash_index) {
                (Some(section), o) => (section, o),
                (None, _) => {
                    return Err(ErrorKind::InvalidArgument {
                        message: "missing hosts".to_string(),
                    }
                    .into())
                }
            },
            None => {
                if after_scheme.find('?').is_some() {
                    return Err(ErrorKind::InvalidArgument {
                        message: "Missing delimiting slash between hosts and options".to_string(),
                    }
                    .into());
                }
                (after_scheme, None)
            }
        };

        let (database, options_section) = match post_slash {
            Some(section) => match section.find('?') {
                Some(index) => exclusive_split_at(section, index),
                None => (post_slash, None),
            },
            None => (None, None),
        };

        let db = match database {
            Some(db) => {
                let decoded = percent_decode(db, "database name must be URL encoded")?;
                if decoded
                    .chars()
                    .any(|c| ILLEGAL_DATABASE_CHARACTERS.contains(&c))
                {
                    return Err(ErrorKind::InvalidArgument {
                        message: "illegal character in database name".to_string(),
                    }
                    .into());
                }
                Some(decoded)
            }
            None => None,
        };

        let (authentication_requested, cred_section, hosts_section) = match pre_slash.rfind('@') {
            Some(index) => {
                // if '@' is in the host section, it MUST be interpreted as a request for
                // authentication, even if the credentials are empty.
                let (creds, hosts) = exclusive_split_at(pre_slash, index);
                match hosts {
                    Some(hs) => (true, creds, hs),
                    None => {
                        return Err(ErrorKind::InvalidArgument {
                            message: "missing hosts".to_string(),
                        }
                        .into())
                    }
                }
            }
            None => (false, None, pre_slash),
        };

        let (username, password) = match cred_section {
            Some(creds) => match creds.find(':') {
                Some(index) => match exclusive_split_at(creds, index) {
                    (username, None) => (username, Some("")),
                    (username, password) => (username, password),
                },
                None => (Some(creds), None), // Lack of ":" implies whole string is username
            },
            None => (None, None),
        };

        let hosts: Result<Vec<_>> = hosts_section.split(',').map(ServerAddress::parse).collect();

        let hosts = hosts?;

        if srv {
            if hosts.len() != 1 {
                return Err(ErrorKind::InvalidArgument {
                    message: "exactly one host must be specified with 'mongodb+srv'".into(),
                }
                .into());
            }

            if hosts[0].port().is_some() {
                return Err(ErrorKind::InvalidArgument {
                    message: "a port cannot be specified with 'mongodb+srv'".into(),
                }
                .into());
            }
        }

        let mut options = ClientOptionsParser {
            hosts,
            srv,
            original_uri: s.into(),
            ..Default::default()
        };

        if let Some(opts) = options_section {
            options.parse_options(opts)?;
        }

        // Set username and password.
        if let Some(u) = username {
            let mut credential = options.credential.get_or_insert_with(Default::default);
            validate_userinfo(u, "username")?;
            let decoded_u = percent_decode(u, "username must be URL encoded")?;

            credential.username = Some(decoded_u);

            if let Some(pass) = password {
                validate_userinfo(pass, "password")?;
                let decoded_p = percent_decode(pass, "password must be URL encoded")?;
                credential.password = Some(decoded_p)
            }
        }

        if options.auth_source.as_deref() == Some("") {
            return Err(ErrorKind::InvalidArgument {
                message: "empty authSource provided".to_string(),
            }
            .into());
        }

        let db_str = db.as_deref();

        match options.auth_mechanism {
            Some(ref mechanism) => {
                let mut credential = options.credential.get_or_insert_with(Default::default);

                credential.source = options
                    .auth_source
                    .clone()
                    .or_else(|| Some(mechanism.default_source(db_str).into()));

                if let Some(mut doc) = options.auth_mechanism_properties.take() {
                    match doc.remove("CANONICALIZE_HOST_NAME") {
                        Some(Bson::String(s)) => {
                            let val = match &s.to_lowercase()[..] {
                                "true" => Bson::Boolean(true),
                                "false" => Bson::Boolean(false),
                                _ => Bson::String(s),
                            };
                            doc.insert("CANONICALIZE_HOST_NAME", val);
                        }
                        Some(val) => {
                            doc.insert("CANONICALIZE_HOST_NAME", val);
                        }
                        None => {}
                    }

                    credential.mechanism_properties = Some(doc);
                }

                mechanism.validate_credential(credential)?;
                credential.mechanism = options.auth_mechanism.take();
            }
            None => {
                if let Some(ref mut credential) = options.credential {
                    // If credentials exist (i.e. username is specified) but no mechanism, the
                    // default source is chosen from the following list in
                    // order (skipping null ones): authSource option, connection string db,
                    // SCRAM default (i.e. "admin").
                    credential.source = options
                        .auth_source
                        .clone()
                        .or(db)
                        .or_else(|| Some("admin".into()));
                } else if authentication_requested {
                    return Err(ErrorKind::InvalidArgument {
                        message: "username and mechanism both not provided, but authentication \
                                  was requested"
                            .to_string(),
                    }
                    .into());
                }
            }
        };

        if options.tls.is_none() && options.srv {
            options.tls = Some(Tls::Enabled(Default::default()));
        }

        Ok(options)
    }

    fn parse_options(&mut self, options: &str) -> Result<()> {
        if options.is_empty() {
            return Ok(());
        }

        let mut keys: Vec<&str> = Vec::new();

        for option_pair in options.split('&') {
            let (key, value) = match option_pair.find('=') {
                Some(index) => option_pair.split_at(index),
                None => {
                    return Err(ErrorKind::InvalidArgument {
                        message: format!(
                            "connection string options is not a `key=value` pair: {}",
                            option_pair,
                        ),
                    }
                    .into())
                }
            };

            if key.to_lowercase() != "readpreferencetags" && keys.contains(&key) {
                return Err(ErrorKind::InvalidArgument {
                    message: "repeated options are not allowed in the connection string"
                        .to_string(),
                }
                .into());
            } else {
                keys.push(key);
            }

            // Skip leading '=' in value.
            self.parse_option_pair(
                &key.to_lowercase(),
                percent_encoding::percent_decode(&value.as_bytes()[1..])
                    .decode_utf8_lossy()
                    .as_ref(),
            )?;
        }

        if let Some(tags) = self.read_preference_tags.take() {
            self.read_preference = match self.read_preference.take() {
                Some(read_pref) => Some(read_pref.with_tags(tags)?),
                None => {
                    return Err(ErrorKind::InvalidArgument {
                        message: "cannot set read preference tags without also setting read \
                                  preference mode"
                            .to_string(),
                    }
                    .into())
                }
            };
        }

        if let Some(max_staleness) = self.max_staleness.take() {
            self.read_preference = match self.read_preference.take() {
                Some(read_pref) => Some(read_pref.with_max_staleness(max_staleness)?),
                None => {
                    return Err(ErrorKind::InvalidArgument {
                        message: "cannot set max staleness without also setting read preference \
                                  mode"
                            .to_string(),
                    }
                    .into())
                }
            };
        }

        self.selection_criteria = self.read_preference.take().map(Into::into);

        if let Some(true) = self.direct_connection {
            if self.srv {
                return Err(ErrorKind::InvalidArgument {
                    message: "cannot use SRV-style URI with directConnection=true".to_string(),
                }
                .into());
            }
        }

        // If zlib and zlib_compression_level are specified then write zlib_compression_level into
        // zlib enum
        if let (Some(compressors), Some(zlib_compression_level)) =
            (self.compressors.as_mut(), self.zlib_compression)
        {
            for compressor in compressors {
                compressor.write_zlib_level(zlib_compression_level)
            }
        }

        Ok(())
    }

    fn parse_option_pair(&mut self, key: &str, value: &str) -> Result<()> {
        macro_rules! get_bool {
            ($value:expr, $option:expr) => {
                match $value {
                    "true" => true,
                    "false" => false,
                    _ => {
                        return Err(ErrorKind::InvalidArgument {
                            message: format!(
                                "connection string `{}` option must be a boolean",
                                $option,
                            ),
                        }
                        .into())
                    }
                }
            };
        }

        macro_rules! get_duration {
            ($value:expr, $option:expr) => {
                match $value.parse::<u64>() {
                    Ok(i) => i,
                    _ => {
                        return Err(ErrorKind::InvalidArgument {
                            message: format!(
                                "connection string `{}` option must be a non-negative integer",
                                $option
                            ),
                        }
                        .into())
                    }
                }
            };
        }

        macro_rules! get_u32 {
            ($value:expr, $option:expr) => {
                match value.parse::<u32>() {
                    Ok(u) => u,
                    Err(_) => {
                        return Err(ErrorKind::InvalidArgument {
                            message: format!(
                                "connection string `{}` argument must be a positive integer",
                                $option,
                            ),
                        }
                        .into())
                    }
                }
            };
        }

        macro_rules! get_i32 {
            ($value:expr, $option:expr) => {
                match value.parse::<i32>() {
                    Ok(u) => u,
                    Err(_) => {
                        return Err(ErrorKind::InvalidArgument {
                            message: format!(
                                "connection string `{}` argument must be an integer",
                                $option
                            ),
                        }
                        .into())
                    }
                }
            };
        }

        match key {
            "appname" => {
                self.app_name = Some(value.into());
            }
            "authmechanism" => {
                self.auth_mechanism = Some(AuthMechanism::from_str(value)?);
            }
            "authsource" => self.auth_source = Some(value.to_string()),
            "authmechanismproperties" => {
                let mut doc = Document::new();
                let err_func = || {
                    ErrorKind::InvalidArgument {
                        message: "improperly formatted authMechanismProperties".to_string(),
                    }
                    .into()
                };

                for kvp in value.split(',') {
                    match kvp.find(':') {
                        Some(index) => {
                            let (k, v) = exclusive_split_at(kvp, index);
                            let key = k.ok_or_else(err_func)?;
                            let value = v.ok_or_else(err_func)?;
                            doc.insert(key, value);
                        }
                        None => return Err(err_func()),
                    };
                }
                self.auth_mechanism_properties = Some(doc);
            }
            "compressors" => {
                let compressors = value
                    .split(',')
                    .filter_map(|x| Compressor::parse_str(x).ok())
                    .collect::<Vec<Compressor>>();
                self.compressors = if compressors.is_empty() {
                    None
                } else {
                    Some(compressors)
                }
            }
            k @ "connecttimeoutms" => {
                self.connect_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "directconnection" => {
                self.direct_connection = Some(get_bool!(value, k));
            }
            k @ "heartbeatfrequencyms" => {
                let duration = get_duration!(value, k);

                if duration < MIN_HEARTBEAT_FREQUENCY.as_millis() as u64 {
                    return Err(ErrorKind::InvalidArgument {
                        message: format!(
                            "'heartbeatFrequencyMS' must be at least 500, but {} was given",
                            duration
                        ),
                    }
                    .into());
                }

                self.heartbeat_freq = Some(Duration::from_millis(duration));
            }
            k @ "journal" => {
                let mut write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.journal = Some(get_bool!(value, k));
            }
            k @ "loadbalanced" => {
                self.load_balanced = Some(get_bool!(value, k));
            }
            k @ "localthresholdms" => {
                self.local_threshold = Some(Duration::from_millis(get_duration!(value, k)))
            }
            k @ "maxidletimems" => {
                self.max_idle_time = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "maxstalenessseconds" => {
                let max_staleness = Duration::from_secs(get_duration!(value, k));

                if max_staleness > Duration::from_secs(0) && max_staleness < Duration::from_secs(90)
                {
                    return Err(ErrorKind::InvalidArgument {
                        message: "'maxStalenessSeconds' cannot be both positive and below 90"
                            .into(),
                    }
                    .into());
                }

                self.max_staleness = Some(max_staleness);
            }
            k @ "maxpoolsize" => {
                self.max_pool_size = Some(get_u32!(value, k));
            }
            k @ "minpoolsize" => {
                self.min_pool_size = Some(get_u32!(value, k));
            }
            "readconcernlevel" => {
                self.read_concern = Some(ReadConcernLevel::from_str(value).into());
            }
            "readpreference" => {
                self.read_preference = Some(match &value.to_lowercase()[..] {
                    "primary" => ReadPreference::Primary,
                    "secondary" => ReadPreference::Secondary {
                        options: Default::default(),
                    },
                    "primarypreferred" => ReadPreference::PrimaryPreferred {
                        options: Default::default(),
                    },
                    "secondarypreferred" => ReadPreference::SecondaryPreferred {
                        options: Default::default(),
                    },
                    "nearest" => ReadPreference::Nearest {
                        options: Default::default(),
                    },
                    other => {
                        return Err(ErrorKind::InvalidArgument {
                            message: format!("'{}' is not a valid read preference", other),
                        }
                        .into())
                    }
                });
            }
            "readpreferencetags" => {
                let tags: Result<TagSet> = if value.is_empty() {
                    Ok(TagSet::new())
                } else {
                    value
                        .split(',')
                        .map(|tag| {
                            let mut values = tag.split(':');

                            match (values.next(), values.next()) {
                                (Some(key), Some(value)) => {
                                    Ok((key.to_string(), value.to_string()))
                                }
                                _ => Err(ErrorKind::InvalidArgument {
                                    message: format!(
                                        "'{}' is not a valid read preference tag (which must be \
                                         of the form 'key:value'",
                                        value,
                                    ),
                                }
                                .into()),
                            }
                        })
                        .collect()
                };

                self.read_preference_tags
                    .get_or_insert_with(Vec::new)
                    .push(tags?);
            }
            "replicaset" => {
                self.repl_set_name = Some(value.to_string());
            }
            k @ "retrywrites" => {
                self.retry_writes = Some(get_bool!(value, k));
            }
            k @ "retryreads" => {
                self.retry_reads = Some(get_bool!(value, k));
            }
            k @ "serverselectiontimeoutms" => {
                self.server_selection_timeout = Some(Duration::from_millis(get_duration!(value, k)))
            }
            k @ "sockettimeoutms" => {
                self.socket_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "tls" | k @ "ssl" => {
                let tls = get_bool!(value, k);

                match (self.tls.as_ref(), tls) {
                    (Some(Tls::Disabled), true) | (Some(Tls::Enabled(..)), false) => {
                        return Err(ErrorKind::InvalidArgument {
                            message: "All instances of `tls` and `ssl` must have the same
 value"
                                .to_string(),
                        }
                        .into());
                    }
                    _ => {}
                };

                if self.tls.is_none() {
                    let tls = if tls {
                        Tls::Enabled(Default::default())
                    } else {
                        Tls::Disabled
                    };

                    self.tls = Some(tls);
                }
            }
            k @ "tlsinsecure" | k @ "tlsallowinvalidcertificates" => {
                let val = get_bool!(value, k);

                let allow_invalid_certificates = if k == "tlsinsecure" { !val } else { val };

                match self.tls {
                    Some(Tls::Disabled) => {
                        return Err(ErrorKind::InvalidArgument {
                            message: "'tlsInsecure' can't be set if tls=false".into(),
                        }
                        .into())
                    }
                    Some(Tls::Enabled(ref options))
                        if options.allow_invalid_certificates.is_some()
                            && options.allow_invalid_certificates
                                != Some(allow_invalid_certificates) =>
                    {
                        return Err(ErrorKind::InvalidArgument {
                            message: "all instances of 'tlsInsecure' and \
                                      'tlsAllowInvalidCertificates' must be consistent (e.g. \
                                      'tlsInsecure' cannot be true when \
                                      'tlsAllowInvalidCertificates' is false, or vice-versa)"
                                .into(),
                        }
                        .into());
                    }
                    Some(Tls::Enabled(ref mut options)) => {
                        options.allow_invalid_certificates = Some(allow_invalid_certificates);
                    }
                    None => {
                        self.tls = Some(Tls::Enabled(
                            TlsOptions::builder()
                                .allow_invalid_certificates(allow_invalid_certificates)
                                .build(),
                        ))
                    }
                }
            }
            "tlscafile" => match self.tls {
                Some(Tls::Disabled) => {
                    return Err(ErrorKind::InvalidArgument {
                        message: "'tlsCAFile' can't be set if tls=false".into(),
                    }
                    .into());
                }
                Some(Tls::Enabled(ref mut options)) => {
                    options.ca_file_path = Some(value.into());
                }
                None => {
                    self.tls = Some(Tls::Enabled(
                        TlsOptions::builder()
                            .ca_file_path(PathBuf::from(value))
                            .build(),
                    ))
                }
            },
            "tlscertificatekeyfile" => match self.tls {
                Some(Tls::Disabled) => {
                    return Err(ErrorKind::InvalidArgument {
                        message: "'tlsCertificateKeyFile' can't be set if tls=false".into(),
                    }
                    .into());
                }
                Some(Tls::Enabled(ref mut options)) => {
                    options.cert_key_file_path = Some(value.into());
                }
                None => {
                    self.tls = Some(Tls::Enabled(
                        TlsOptions::builder()
                            .cert_key_file_path(PathBuf::from(value))
                            .build(),
                    ))
                }
            },
            "w" => {
                let mut write_concern = self.write_concern.get_or_insert_with(Default::default);

                match value.parse::<i32>() {
                    Ok(w) => match u32::try_from(w) {
                        Ok(uw) => write_concern.w = Some(Acknowledgment::from(uw)),
                        Err(_) => {
                            return Err(ErrorKind::InvalidArgument {
                                message: "connection string `w` option cannot be a negative \
                                          integer"
                                    .to_string(),
                            }
                            .into())
                        }
                    },
                    Err(_) => {
                        write_concern.w = Some(Acknowledgment::from(value.to_string()));
                    }
                };
            }
            k @ "waitqueuetimeoutms" => {
                self.wait_queue_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "wtimeoutms" => {
                let write_concern = self.write_concern.get_or_insert_with(Default::default);
                write_concern.w_timeout = Some(Duration::from_millis(get_duration!(value, k)));
            }
            k @ "zlibcompressionlevel" => {
                let i = get_i32!(value, k);
                if i < -1 {
                    return Err(ErrorKind::InvalidArgument {
                        message: "'zlibCompressionLevel' cannot be less than -1".to_string(),
                    }
                    .into());
                }

                if i > 9 {
                    return Err(ErrorKind::InvalidArgument {
                        message: "'zlibCompressionLevel' cannot be greater than 9".to_string(),
                    }
                    .into());
                }

                self.zlib_compression = Some(i);
            }

            other => {
                let (jaro_winkler, option) = URI_OPTIONS.iter().fold((0.0, ""), |acc, option| {
                    let jaro_winkler = jaro_winkler(option, other).abs();
                    if jaro_winkler > acc.0 {
                        return (jaro_winkler, option);
                    }
                    acc
                });
                let mut message = format!("{} is an invalid option", other);
                if jaro_winkler >= 0.84 {
                    message.push_str(&format!(
                        ". An option with a similar name exists: {}",
                        option
                    ));
                }
                return Err(ErrorKind::InvalidArgument { message }.into());
            }
        }

        Ok(())
    }
}

#[cfg(all(test, not(feature = "sync")))]
mod tests {
    use std::time::Duration;

    use pretty_assertions::assert_eq;

    use super::{ClientOptions, ServerAddress};
    use crate::{
        concern::{Acknowledgment, ReadConcernLevel, WriteConcern},
        selection_criteria::{ReadPreference, ReadPreferenceOptions},
    };

    macro_rules! tag_set {
        ( $($k:expr => $v:expr),* ) => {
            #[allow(clippy::let_and_return)]
            {
                use std::collections::HashMap;

                #[allow(unused_mut)]
                let mut ts = HashMap::new();
                $(
                    ts.insert($k.to_string(), $v.to_string());
                )*

                ts
            }
        }
    }

    fn host_without_port(hostname: &str) -> ServerAddress {
        ServerAddress::Tcp {
            host: hostname.to_string(),
            port: None,
        }
    }

    #[test]
    fn test_parse_address_with_from_str() {
        let x = "localhost:27017".parse::<ServerAddress>().unwrap();
        let ServerAddress::Tcp { host, port } = x;
        assert_eq!(host, "localhost");
        assert_eq!(port, Some(27017));

        // Port defaults to 27017 (so this doesn't fail)
        let x = "localhost".parse::<ServerAddress>().unwrap();
        let ServerAddress::Tcp { host, port } = x;
        assert_eq!(host, "localhost");
        assert_eq!(port, None);

        let x = "localhost:not a number".parse::<ServerAddress>();
        assert!(x.is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn fails_without_scheme() {
        assert!(ClientOptions::parse("localhost:27017").await.is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn fails_with_invalid_scheme() {
        assert!(ClientOptions::parse("mangodb://localhost:27017")
            .await
            .is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn fails_with_nothing_after_scheme() {
        assert!(ClientOptions::parse("mongodb://").await.is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn fails_with_only_slash_after_scheme() {
        assert!(ClientOptions::parse("mongodb:///").await.is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn fails_with_no_host() {
        assert!(ClientOptions::parse("mongodb://:27017").await.is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn no_port() {
        let uri = "mongodb://localhost";

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![host_without_port("localhost")],
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn no_port_trailing_slash() {
        let uri = "mongodb://localhost/";

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![host_without_port("localhost")],
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_port() {
        let uri = "mongodb://localhost/";

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_port_and_trailing_slash() {
        let uri = "mongodb://localhost:27017/";

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_read_concern() {
        let uri = "mongodb://localhost:27017/?readConcernLevel=foo";

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                read_concern: Some(ReadConcernLevel::Custom("foo".to_string()).into()),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_w_negative_int() {
        assert!(ClientOptions::parse("mongodb://localhost:27017/?w=-1")
            .await
            .is_err());
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_w_non_negative_int() {
        let uri = "mongodb://localhost:27017/?w=1";
        let write_concern = WriteConcern::builder().w(Acknowledgment::from(1)).build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_w_string() {
        let uri = "mongodb://localhost:27017/?w=foo";
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::from("foo".to_string()))
            .build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_invalid_j() {
        assert!(
            ClientOptions::parse("mongodb://localhost:27017/?journal=foo")
                .await
                .is_err()
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_j() {
        let uri = "mongodb://localhost:27017/?journal=true";
        let write_concern = WriteConcern::builder().journal(true).build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_wtimeout_non_int() {
        assert!(
            ClientOptions::parse("mongodb://localhost:27017/?wtimeoutMS=foo")
                .await
                .is_err()
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_wtimeout_negative_int() {
        assert!(
            ClientOptions::parse("mongodb://localhost:27017/?wtimeoutMS=-1")
                .await
                .is_err()
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_wtimeout() {
        let uri = "mongodb://localhost:27017/?wtimeoutMS=27";
        let write_concern = WriteConcern::builder()
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_all_write_concern_options() {
        let uri = "mongodb://localhost:27017/?w=majority&journal=false&wtimeoutMS=27";
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::Majority)
            .journal(false)
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![ServerAddress::Tcp {
                    host: "localhost".to_string(),
                    port: Some(27017),
                }],
                write_concern: Some(write_concern),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_invalid_read_preference_mode() {}

    #[cfg_attr(feature = "tokio-runtime", tokio::test)]
    #[cfg_attr(feature = "async-std-runtime", async_std::test)]
    async fn with_mixed_options() {
        let uri = "mongodb://localhost,localhost:27018/?w=majority&readConcernLevel=majority&\
                   journal=false&wtimeoutMS=27&replicaSet=foo&heartbeatFrequencyMS=1000&\
                   localThresholdMS=4000&readPreference=secondaryPreferred&readpreferencetags=dc:\
                   ny,rack:1&serverselectiontimeoutms=2000&readpreferencetags=dc:ny&\
                   readpreferencetags=";
        let write_concern = WriteConcern::builder()
            .w(Acknowledgment::Majority)
            .journal(false)
            .w_timeout(Duration::from_millis(27))
            .build();

        assert_eq!(
            ClientOptions::parse(uri).await.unwrap(),
            ClientOptions {
                hosts: vec![
                    ServerAddress::Tcp {
                        host: "localhost".to_string(),
                        port: None,
                    },
                    ServerAddress::Tcp {
                        host: "localhost".to_string(),
                        port: Some(27018),
                    },
                ],
                selection_criteria: Some(
                    ReadPreference::SecondaryPreferred {
                        options: ReadPreferenceOptions::builder()
                            .tag_sets(vec![
                                tag_set! {
                                    "dc" => "ny",
                                    "rack" => "1"
                                },
                                tag_set! {
                                    "dc" => "ny"
                                },
                                tag_set! {},
                            ])
                            .build()
                    }
                    .into()
                ),
                read_concern: Some(ReadConcernLevel::Majority.into()),
                write_concern: Some(write_concern),
                repl_set_name: Some("foo".to_string()),
                heartbeat_freq: Some(Duration::from_millis(1000)),
                local_threshold: Some(Duration::from_millis(4000)),
                server_selection_timeout: Some(Duration::from_millis(2000)),
                original_uri: Some(uri.into()),
                ..Default::default()
            }
        );
    }
}

/// Contains the options that can be used to create a new
/// [`ClientSession`](../struct.ClientSession.html).
#[derive(Clone, Debug, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SessionOptions {
    /// The default options to use for transactions started on this session.
    ///
    /// If these options are not specified, they will be inherited from the
    /// [`Client`](../struct.Client.html) associated with this session. They will not
    /// be inherited from the options specified
    /// on the [`Database`](../struct.Database.html) or [`Collection`](../struct.Collection.html)
    /// associated with the operations within the transaction.
    pub default_transaction_options: Option<TransactionOptions>,

    /// If true, all read operations performed using this client session will share the same
    /// snapshot.  Defaults to false.
    // TODO RUST-18 enforce snapshot exclusivity with causalConsistency.
    pub snapshot: Option<bool>,
}

/// Contains the options that can be used for a transaction.
#[skip_serializing_none]
#[derive(Debug, Default, Serialize, Deserialize, TypedBuilder, Clone)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct TransactionOptions {
    /// The read concern to use for the transaction.
    #[builder(default)]
    #[serde(skip_serializing)]
    pub read_concern: Option<ReadConcern>,

    /// The write concern to use when committing or aborting a transaction.
    #[builder(default)]
    pub write_concern: Option<WriteConcern>,

    /// The selection criteria to use for all read operations in a transaction.
    #[builder(default)]
    #[serde(skip_serializing, rename = "readPreference")]
    pub selection_criteria: Option<SelectionCriteria>,

    /// The maximum amount of time to allow a single commitTransaction to run.
    #[builder(default)]
    #[serde(
        serialize_with = "bson_util::serialize_duration_option_as_int_millis",
        deserialize_with = "bson_util::deserialize_duration_option_from_u64_millis",
        rename(serialize = "maxTimeMS", deserialize = "maxCommitTimeMS"),
        default
    )]
    pub max_commit_time: Option<Duration>,
}
