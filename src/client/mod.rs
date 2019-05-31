#[cfg(test)]
mod test;

use std::{
    cmp::Ordering,
    sync::{Arc, RwLock},
    time::Duration,
};

use bson::{Bson, Document};
use derivative::Derivative;
use rand::{seq::SliceRandom, thread_rng};
use time::{Duration as TimeDuration, PreciseTime};

use crate::{
    command_responses::ListDatabasesResponse,
    concern::{ReadConcern, WriteConcern},
    connstring::ConnectionString,
    db::Database,
    error::{ErrorKind, Result},
    options::DatabaseOptions,
    pool::Connection,
    read_preference::ReadPreference,
    topology::{ServerDescription, ServerType, Topology, TopologyType},
};

lazy_static! {
    static ref DEFAULT_LOCAL_THRESHOLD: i64 = 15;
    static ref DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed.
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// # use mongodb::{Client, error::Result};
///
/// # fn start_workers() -> Result<()> {
/// let client = Client::with_uri("mongodb://example.com")?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     std::thread::spawn(move || {
///         let collection = client_ref.database("items").collection(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # // Technically we should join the threads here, but for the purpose of the example, we'll just
/// # // sleep for a bit.
/// # std::thread::sleep(std::time::Duration::from_secs(3));
/// # Ok(())
/// # }
/// ```
#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Arc<RwLock<Topology>>,
    #[derivative(Debug = "ignore")]
    tls_config: Option<Arc<rustls::ClientConfig>>,
    read_preference: Option<ReadPreference>,
    read_concern: Option<ReadConcern>,
    write_concern: Option<WriteConcern>,
    local_threshold: Option<i64>,
    server_selection_timeout: Option<Duration>,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    pub fn with_uri(uri: &str) -> Result<Self> {
        let mut connstring = ConnectionString::parse(uri)?;
        let tls_config = match connstring.tls_options.take() {
            Some(opts) => Some(Arc::new(opts.into_rustls_config()?)),
            None => None,
        };

        Ok(Self {
            inner: Arc::new(ClientInner {
                tls_config: tls_config.clone(),
                local_threshold: connstring.local_threshold,
                server_selection_timeout: connstring.server_selection_timeout,
                read_preference: connstring.read_preference.take(),
                read_concern: connstring.read_concern.take(),
                write_concern: connstring.write_concern.take(),
                topology: Topology::new(connstring, tls_config),
            }),
        })
    }

    /// Gets the read concern of the `Client`.
    pub fn read_preference(&self) -> Option<&ReadPreference> {
        self.inner.read_preference.as_ref()
    }

    /// Gets the read concern of the `Client`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.read_concern.as_ref()
    }

    /// Gets the write concern of the `Client`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern.as_ref()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        self.database_with_options(name, Default::default())
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// Operations done with this `Database` will use the options specified by `options` by default
    /// and will otherwise default to those of the `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database_with_options(&self, name: &str, options: DatabaseOptions) -> Database {
        Database::new(self.clone(), name, Some(options))
    }

    pub(crate) fn slave_ok(&self, address: &str, read_preference: Option<&ReadPreference>) -> bool {
        self.inner
            .topology
            .read()
            .unwrap()
            .slave_ok(address, read_preference.or_else(|| self.read_preference()))
    }

    /// Gets information about each of the databases in the connected server in the form of an
    /// an array of documents.
    pub fn list_databases(&self, filter: Option<Document>) -> Result<Vec<Document>> {
        let mut cmd = doc! { "listDatabases" : 1 };
        if let Some(filter) = filter {
            cmd.insert("filter", filter);
        }

        let result = self.list_databases_command(cmd)?;
        Ok(result.databases)
    }

    /// Gets the names of the databases in the connected server.
    pub fn list_database_names(&self, filter: Option<Document>) -> Result<Vec<String>> {
        let mut cmd = doc! {
            "listDatabases" : 1,
            "nameOnly" : true
        };
        if let Some(filter) = filter {
            cmd.insert("filter", filter);
        }

        let result = self.list_databases_command(cmd)?;
        result
            .databases
            .into_iter()
            .map(|database| match database.get("name") {
                Some(Bson::String(name)) => Ok(name.to_string()),
                _ => bail!(ErrorKind::ResponseError(
                    "invalid document returned by listDatabases command".to_string(),
                )),
            })
            .collect()
    }

    fn list_databases_command(&self, cmd: Document) -> Result<ListDatabasesResponse> {
        let (_, result) = self.database("admin").run_driver_command(cmd, None, None)?;

        match bson::from_bson(Bson::Document(result)) {
            Ok(response) => Ok(response),
            Err(_) => bail!(ErrorKind::ResponseError(
                "invalid server response to listDatabases command".to_string()
            )),
        }
    }

    fn server_selection_timeout(&self) -> Duration {
        self.inner
            .server_selection_timeout
            .unwrap_or(*DEFAULT_SERVER_SELECTION_TIMEOUT)
    }

    fn get_connection_from_server(&self, address: &str) -> Result<Option<Connection>> {
        self.inner
            .topology
            .read()
            .unwrap()
            .get_server(address)
            .map(|server| server.read().unwrap().acquire_stream())
            .transpose()
    }

    fn server_selection_timeout_exceeded(&self, start_time: PreciseTime) -> bool {
        let timeout = TimeDuration::from_std(self.server_selection_timeout()).unwrap();
        start_time.to(PreciseTime::now()) > timeout
    }

    fn acquire_stream_from_address(&self, address: &str) -> Result<Connection> {
        let start_time = PreciseTime::now();

        loop {
            if let Some(conn) = self.get_connection_from_server(address)? {
                return Ok(conn);
            }

            if self.server_selection_timeout_exceeded(start_time) {
                bail!(ErrorKind::ServerSelectionError(format!(
                    "Unable to obtain connection from server {}",
                    address
                )));
            }
        }
    }

    pub(crate) fn acquire_stream(
        &self,
        read_pref: Option<&ReadPreference>,
        address: Option<&str>,
    ) -> Result<(String, Connection)> {
        if let Some(address) = address {
            return self
                .acquire_stream_from_address(address)
                .map(|conn| (address.to_string(), conn));
        }

        let read_pref = read_pref.or_else(|| self.inner.read_preference.as_ref());

        let local_threshold = self
            .inner
            .local_threshold
            .unwrap_or(*DEFAULT_LOCAL_THRESHOLD);

        let start_time = PreciseTime::now();

        loop {
            let (server_descriptions, topology_type) = {
                let topology_lock = self.inner.topology.read().unwrap();

                if let Some(msg) = topology_lock.compatibility_error() {
                    bail!(ErrorKind::ServerSelectionError(msg.to_string()));
                }

                let topology_type = topology_lock.topology_type();
                let server_descriptions = topology_lock.server_descriptions();

                (server_descriptions, topology_type)
            };

            let mut servers = server_descriptions.into_iter().map(|kv| kv.1).collect();

            retain_suitable_servers(&mut servers, topology_type, read_pref);
            retain_within_latency_window(&mut servers, local_threshold);

            servers.shuffle(&mut thread_rng());

            // If we can't get a connection to a server that was picked, that means the server we
            // selected has been removed from the topology between when we obtained the
            // list of server descriptions and now, so remove that address and try to
            // pick another. If we can't get a connection from any of the servers, we're
            // forced to start from scratch.
            for server in servers.drain(..) {
                if let Some(conn) = self.get_connection_from_server(&server.address)? {
                    return Ok((server.address, conn));
                }
            }

            if self.server_selection_timeout_exceeded(start_time) {
                bail!(ErrorKind::ServerSelectionError(match read_pref {
                    Some(read_pref) => {
                        format!("No server available with ReadPreference {}", read_pref)
                    }
                    None => "No server available".to_string(),
                }));
            }
        }
    }
}

fn retain_suitable_servers(
    servers: &mut Vec<ServerDescription>,
    topology_type: TopologyType,
    read_pref: Option<&ReadPreference>,
) {
    servers.retain(|s| match topology_type {
        TopologyType::Single => true,
        TopologyType::ReplicaSetNoPrimary | TopologyType::ReplicaSetWithPrimary => {
            s.round_trip_time.is_some() && s.matches(topology_type, read_pref)
        }
        TopologyType::Sharded => s.server_type == ServerType::Mongos,
        TopologyType::Unknown => false,
    });

    if topology_type.is_replica_set()
        && read_pref
            .map(ReadPreference::is_secondary_preferred)
            .unwrap_or(false)
        && servers
            .iter()
            .any(|s| s.server_type == ServerType::RSSecondary)
    {
        servers.retain(|s| s.server_type == ServerType::RSSecondary);
    }
}

fn retain_within_latency_window(servers: &mut Vec<ServerDescription>, local_threshold: i64) {
    let min_round_trip_time = servers
        .iter()
        .filter_map(|s| s.round_trip_time)
        .min_by(|x, y| x.partial_cmp(y).unwrap_or(Ordering::Equal));

    if let Some(min) = min_round_trip_time {
        servers.retain(|s| s.round_trip_time.unwrap() <= min + local_threshold as f64);
    }
}
