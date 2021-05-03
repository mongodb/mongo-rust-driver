pub mod auth;
mod executor;
pub mod options;
pub mod session;

use std::{sync::Arc, time::Duration};

use bson::Bson;
use derivative::Derivative;
use std::time::Instant;

#[cfg(test)]
use crate::options::StreamAddress;
use crate::{
    bson::Document,
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{ErrorKind, Result},
    event::command::CommandEventHandler,
    operation::ListDatabases,
    options::{
        ClientOptions,
        DatabaseOptions,
        ListDatabasesOptions,
        ReadPreference,
        SelectionCriteria,
        SessionOptions,
    },
    results::DatabaseSpecification,
    sdam::{SelectedServer, SessionSupportStatus, Topology},
    ClientSession,
};
pub(crate) use session::{ClusterTime, SESSIONS_UNSUPPORTED_COMMANDS};
use session::{ServerSession, ServerSessionPool};

const DEFAULT_SERVER_SELECTION_TIMEOUT: Duration = Duration::from_secs(30);

/// This is the main entry point for the API. A `Client` is used to connect to a MongoDB cluster.
/// By default, it will monitor the topology of the cluster, keeping track of any changes, such
/// as servers being added or removed.
///
/// `Client` uses [`std::sync::Arc`](https://doc.rust-lang.org/std/sync/struct.Arc.html) internally,
/// so it can safely be shared across threads or async tasks. For example:
///
/// ```rust
/// # #[cfg(not(feature = "sync"))]
/// # use mongodb::{bson::Document, Client, error::Result};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # #[cfg(not(feature = "sync"))]
/// # async fn start_workers() -> Result<()> {
/// let client = Client::with_uri_str("mongodb://example.com").await?;
///
/// for i in 0..5 {
///     let client_ref = client.clone();
///
///     task::spawn(async move {
///         let collection = client_ref.database("items").collection::<Document>(&format!("coll{}", i));
///
///         // Do something with the collection
///     });
/// }
/// #
/// # Ok(())
/// # }
/// ```
///
/// ## TCP Keepalive
/// TCP keepalive is enabled by default with ``tcp_keepalive_time`` set to 120 seconds. The
/// driver does not set ``tcp_keepalive_intvl``. See the
/// [MongoDB Diagnostics FAQ keepalive section](https://docs.mongodb.com/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments)
/// for instructions on setting these values at the system level.
#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<ClientInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Topology,
    options: ClientOptions,
    session_pool: ServerSessionPool,
}

impl Drop for ClientInner {
    fn drop(&mut self) {
        self.topology.mark_closed()
    }
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    ///
    /// See the documentation on
    /// [`ClientOptions::parse`](options/struct.ClientOptions.html#method.parse) for more details.
    pub async fn with_uri_str(uri: &str) -> Result<Self> {
        let options = ClientOptions::parse_uri(uri, None).await?;

        Client::with_options(options)
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub fn with_options(options: ClientOptions) -> Result<Self> {
        options.validate()?;

        let inner = Arc::new(ClientInner {
            topology: Topology::new(options.clone())?,
            session_pool: ServerSessionPool::new(),
            options,
        });

        Ok(Self { inner })
    }

    pub(crate) fn emit_command_event(&self, emit: impl FnOnce(&Arc<dyn CommandEventHandler>)) {
        if let Some(ref handler) = self.inner.options.command_event_handler {
            emit(handler);
        }
    }

    /// Gets the default selection criteria the `Client` uses for operations..
    pub fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.options.selection_criteria.as_ref()
    }

    /// Gets the default read concern the `Client` uses for operations.
    pub fn read_concern(&self) -> Option<&ReadConcern> {
        self.inner.options.read_concern.as_ref()
    }

    /// Gets the default write concern the `Client` uses for operations.
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

    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
    ) -> Result<Vec<DatabaseSpecification>> {
        let op = ListDatabases::new(filter.into(), false, options.into());
        self.execute_operation(op, None).await.and_then(|dbs| {
            dbs.into_iter()
                .map(|db_spec| bson::from_document(db_spec).map_err(crate::error::Error::from))
                .collect()
        })
    }

    /// Gets the names of the databases present in the cluster the Client is connected to.
    pub async fn list_database_names(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
    ) -> Result<Vec<String>> {
        let op = ListDatabases::new(filter.into(), true, options.into());
        match self.execute_operation(op, None).await {
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

    /// Starts a new `ClientSession`.
    pub async fn start_session(&self, options: Option<SessionOptions>) -> Result<ClientSession> {
        match self.get_session_support_status().await? {
            SessionSupportStatus::Supported {
                logical_session_timeout,
            } => Ok(self
                .start_session_with_timeout(logical_session_timeout, options, false)
                .await),
            _ => Err(ErrorKind::SessionsNotSupported.into()),
        }
    }

    /// Check in a server session to the server session pool.
    /// If the session is expired or dirty, or the topology no longer supports sessions, the session
    /// will be discarded.
    pub(crate) async fn check_in_server_session(&self, session: ServerSession) {
        let session_support_status = self.inner.topology.session_support_status().await;
        if let SessionSupportStatus::Supported {
            logical_session_timeout,
        } = session_support_status
        {
            self.inner
                .session_pool
                .check_in(session, logical_session_timeout)
                .await;
        }
    }

    /// Starts a `ClientSession`.
    ///
    /// This method will attempt to re-use server sessions from the pool which are not about to
    /// expire according to the provided logical session timeout. If no such sessions are
    /// available, a new one will be created.
    pub(crate) async fn start_session_with_timeout(
        &self,
        logical_session_timeout: Duration,
        options: Option<SessionOptions>,
        is_implicit: bool,
    ) -> ClientSession {
        ClientSession::new(
            self.inner
                .session_pool
                .check_out(logical_session_timeout)
                .await,
            self.clone(),
            options,
            is_implicit,
        )
    }

    #[cfg(test)]
    pub(crate) async fn clear_session_pool(&self) {
        self.inner.session_pool.clear().await;
    }

    #[cfg(test)]
    pub(crate) async fn is_session_checked_in(&self, id: &Document) -> bool {
        self.inner.session_pool.contains(id).await
    }

    /// Get the address of the server selected according to the given criteria.
    /// This method is only used in tests.
    #[cfg(test)]
    pub(crate) async fn test_select_server(
        &self,
        criteria: Option<&SelectionCriteria>,
    ) -> Result<StreamAddress> {
        let server = self.select_server(criteria).await?;
        Ok(server.address.clone())
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    async fn select_server(&self, criteria: Option<&SelectionCriteria>) -> Result<SelectedServer> {
        let criteria =
            criteria.unwrap_or(&SelectionCriteria::ReadPreference(ReadPreference::Primary));

        let start_time = Instant::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);

        loop {
            let mut topology_change_subscriber =
                self.inner.topology.subscribe_to_topology_changes();

            let selected_server = self
                .inner
                .topology
                .attempt_to_select_server(criteria)
                .await?;

            if let Some(server) = selected_server {
                return Ok(server);
            }

            self.inner.topology.request_topology_check();

            // If the time that has passed since the start of the loop is greater than the timeout,
            // then `time_remaining` will be 0, so no change will be found.
            let time_passed = start_time.elapsed();
            let time_remaining = timeout
                .checked_sub(time_passed)
                .unwrap_or_else(|| Duration::from_millis(0));

            let change_occurred = topology_change_subscriber
                .wait_for_message(time_remaining)
                .await;

            if !change_occurred {
                return Err(ErrorKind::ServerSelectionError {
                    message: self
                        .inner
                        .topology
                        .server_selection_timeout_error_message(&criteria)
                        .await,
                }
                .into());
            }
        }
    }

    #[cfg(all(test, not(feature = "sync")))]
    pub(crate) async fn get_hosts(&self) -> Vec<String> {
        let servers = self.inner.topology.servers().await;
        servers
            .iter()
            .map(|stream_address| format!("{}", stream_address))
            .collect()
    }
}
