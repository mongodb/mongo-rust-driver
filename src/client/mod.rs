pub mod auth;
mod executor;
pub mod options;

use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use time::PreciseTime;

use bson::{Bson, Document};
use derivative::Derivative;

use crate::{
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{ErrorKind, Result},
    event::command::{
        CommandEventHandler, CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent,
    },
    operation::ListDatabases,
    options::{ClientOptions, DatabaseOptions},
    sdam::{Server, Topology, TopologyUpdateCondvar},
    selection_criteria::{ReadPreference, SelectionCriteria},
};

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads. For example:
///
/// ```rust
/// 
/// # use mongodb::{Client, error::Result};
///
/// # fn start_workers() -> Result<()> {
/// let client = Client::with_uri_str("mongodb://example.com")?;
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
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Arc<RwLock<Topology>>,
    options: ClientOptions,
    #[derivative(Debug = "ignore")]
    condvar: TopologyUpdateCondvar,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    pub fn with_uri_str(uri: &str) -> Result<Self> {
        let options = ClientOptions::parse(uri)?;

        Client::with_options(options)
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub fn with_options(options: ClientOptions) -> Result<Self> {
        let condvar = TopologyUpdateCondvar::new();

        let inner = Arc::new(ClientInner {
            topology: Topology::new(condvar.clone(), options.clone())?,
            condvar,
            options,
        });

        Ok(Self { inner })
    }

    fn emit_command_event(&self, emit: impl FnOnce(&Arc<dyn CommandEventHandler>)) {
        if let Some(ref handler) = self.inner.options.command_event_handler {
            emit(handler);
        }
    }

    /// Gets the read preference of the `Client`.
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the read concern of the `Client`.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the write concern of the `Client`.
    pub fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.options.write_concern.as_ref()
    }

    /// Gets a handle to a database specified by `name` in the cluster the `Client` is connected to.
    /// The `Database` options (e.g. read preference and write concern) will default to those of the
    /// `Client`.
    ///
    /// This method does not send or receive anything across the wire to the database, so it can be
    /// used repeatedly without incurring any costs from I/O.
    pub fn database(&self, name: &str) -> Database {
        Database::new(self.clone(), name, None)
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

    pub fn list_databases(&self, filter: impl Into<Option<Document>>) -> Result<Vec<Document>> {
        let op = ListDatabases::new(filter.into(), false);
        self.execute_operation(&op, None)
    }

    pub fn list_database_names(&self, filter: impl Into<Option<Document>>) -> Result<Vec<String>> {
        let op = ListDatabases::new(filter.into(), true);
        match self.execute_operation(&op, None) {
            Ok(databases) => databases
                .into_iter()
                .map(|doc| {
                    let name = doc.get("name").and_then(Bson::as_str).ok_or_else(|| {
                        ErrorKind::ResponseError {
                            message: "Expected \"name\" field in server response, but it was not \
                                      found"
                                .to_string(),
                        }
                    })?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(e) => Err(e),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn send_command_started_event(&self, event: CommandStartedEvent) {
        self.emit_command_event(|handler| handler.handle_command_started_event(event.clone()));
    }

    #[allow(dead_code)]
    pub(crate) fn send_command_succeeded_event(&self, event: CommandSucceededEvent) {
        self.emit_command_event(|handler| handler.handle_command_succeeded_event(event.clone()));
    }

    #[allow(dead_code)]
    pub(crate) fn send_command_failed_event(&self, event: CommandFailedEvent) {
        self.emit_command_event(|handler| handler.handle_command_failed_event(event.clone()));
    }

    fn topology(&self) -> Arc<RwLock<Topology>> {
        self.inner.topology.clone()
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    fn select_server(&self, criteria: Option<&SelectionCriteria>) -> Result<Arc<Server>> {
        let criteria =
            criteria.unwrap_or_else(|| &SelectionCriteria::ReadPreference(ReadPreference::Primary));
        let start_time = PreciseTime::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);

        while start_time.to(PreciseTime::now()).to_std().unwrap() < timeout {
            // Because we're calling clone on the lock guard, we're actually copying the
            // Topology itself, not just making a new reference to it. The
            // `servers` field will contain references to the same instances
            // though, since each is wrapped in an `Arc`.
            let topology = self.inner.topology.read().unwrap().clone();

            // Return error if the wire version is invalid.
            if let Some(error_msg) = topology.description.compatibility_error() {
                return Err(ErrorKind::ServerSelectionError {
                    message: error_msg.into(),
                }
                .into());
            }

            let server = topology
                .description
                .select_server(&criteria)?
                .and_then(|server| topology.servers.get(&server.address));

            if let Some(server) = server {
                return Ok(server.clone());
            }

            // Because the servers in the copied Topology are Arc aliases of the servers in the
            // original Topology, requesting a check on the copy will in turn request a check from
            // each of the original servers, so the monitoring threads will be woken the same way
            // they would if `request_topology_check` were called on the original Topology.
            topology.request_topology_check();

            self.inner
                .condvar
                .wait_timeout(timeout - start_time.to(PreciseTime::now()).to_std().unwrap());
        }

        Err(ErrorKind::ServerSelectionError {
            message: "timed out while trying to select server".into(),
        }
        .into())
    }
}
