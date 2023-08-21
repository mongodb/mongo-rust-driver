pub mod auth;
#[cfg(feature = "in-use-encryption-unstable")]
pub(crate) mod csfle;
mod executor;
pub mod options;
pub mod session;

use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex as SyncMutex,
    },
    time::{Duration, Instant},
};

#[cfg(feature = "in-use-encryption-unstable")]
pub use self::csfle::client_builder::*;
use derivative::Derivative;
use futures_core::{future::BoxFuture, Future};
use futures_util::{future::join_all, FutureExt};

#[cfg(test)]
use crate::options::ServerAddress;
#[cfg(feature = "tracing-unstable")]
use crate::trace::{
    command::CommandTracingEventEmitter,
    server_selection::ServerSelectionTracingEventEmitter,
    trace_or_log_enabled,
    TracingOrLogLevel,
    COMMAND_TRACING_EVENT_TARGET,
};
use crate::{
    bson::Document,
    change_stream::{
        event::ChangeStreamEvent,
        options::ChangeStreamOptions,
        session::SessionChangeStream,
        ChangeStream,
    },
    concern::{ReadConcern, WriteConcern},
    db::Database,
    error::{Error, ErrorKind, Result},
    event::command::{handle_command_event, CommandEvent},
    id_set::IdSet,
    operation::{AggregateTarget, ListDatabases},
    options::{
        ClientOptions,
        DatabaseOptions,
        ListDatabasesOptions,
        ReadPreference,
        SelectionCriteria,
        SessionOptions,
    },
    results::DatabaseSpecification,
    sdam::{server_selection, SelectedServer, Topology},
    tracking_arc::TrackingArc,
    ClientSession,
};

pub(crate) use executor::{HELLO_COMMAND_NAMES, REDACTED_COMMANDS};
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
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
/// # use mongodb::{bson::Document, Client, error::Result};
/// # #[cfg(feature = "async-std-runtime")]
/// # use async_std::task;
/// # #[cfg(feature = "tokio-runtime")]
/// # use tokio::task;
/// #
/// # #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
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
/// ## Notes on performance
/// Spawning many asynchronous tasks that use the driver concurrently like this is often the best
/// way to achieve maximum performance, as the driver is designed to work well in such situations.
///
/// Additionally, using a custom Rust type that implements `Serialize` and `Deserialize` as the
/// generic parameter of [`Collection`](../struct.Collection.html) instead of [`bson::Document`] can
/// reduce the amount of time the driver and your application spends serializing and deserializing
/// BSON, which can also lead to increased performance.
///
/// ## TCP Keepalive
/// TCP keepalive is enabled by default with ``tcp_keepalive_time`` set to 120 seconds. The
/// driver does not set ``tcp_keepalive_intvl``. See the
/// [MongoDB Diagnostics FAQ keepalive section](https://www.mongodb.com/docs/manual/faq/diagnostics/#does-tcp-keepalive-time-affect-mongodb-deployments)
/// for instructions on setting these values at the system level.
///
/// ## Clean shutdown
/// Because Rust has no async equivalent of `Drop`, values that require server-side cleanup when
/// dropped spawn a new async task to perform that cleanup.  This can cause two potential issues:
///
/// * Drop tasks pending or in progress when the async runtime shuts down may not complete, causing
///   server-side resources to not be freed.
/// * Drop tasks may run at an arbitrary time even after no `Client` values exist, making it hard to
///   reason about associated resources (e.g. event handlers).
///
/// To address these issues, we highly recommend you use [`Client::shutdown`] in the termination
/// path of your application.  This will ensure that outstanding resources have been cleaned up and
/// terminate internal worker tasks before returning.  Please note that `shutdown` will wait for
/// _all_ outstanding resource handles to be dropped, so they must either have been dropped before
/// calling `shutdown` or in a concurrent task; see the documentation of `shutdown` for more
/// details.
#[derive(Debug, Clone)]
pub struct Client {
    inner: TrackingArc<ClientInner>,
}

#[allow(dead_code, unreachable_code, clippy::diverging_sub_expression)]
const _: fn() = || {
    fn assert_send<T: Send>(_t: T) {}
    fn assert_sync<T: Sync>(_t: T) {}

    let _c: super::Client = todo!();
    assert_send(_c);
    assert_sync(_c);
};

#[derive(Derivative)]
#[derivative(Debug)]
struct ClientInner {
    topology: Topology,
    options: ClientOptions,
    session_pool: ServerSessionPool,
    shutdown: Shutdown,
    #[cfg(feature = "in-use-encryption-unstable")]
    csfle: tokio::sync::RwLock<Option<csfle::ClientState>>,
}

#[derive(Debug)]
struct Shutdown {
    pending_drops: SyncMutex<IdSet<crate::runtime::AsyncJoinHandle<()>>>,
    executed: AtomicBool,
}

impl Client {
    /// Creates a new `Client` connected to the cluster specified by `uri`. `uri` must be a valid
    /// MongoDB connection string.
    ///
    /// See the documentation on
    /// [`ClientOptions::parse`](options/struct.ClientOptions.html#method.parse) for more details.
    pub async fn with_uri_str(uri: impl AsRef<str>) -> Result<Self> {
        let options = ClientOptions::parse_uri(uri.as_ref(), None).await?;

        Client::with_options(options)
    }

    /// Creates a new `Client` connected to the cluster specified by `options`.
    pub fn with_options(options: ClientOptions) -> Result<Self> {
        options.validate()?;

        let inner = TrackingArc::new(ClientInner {
            topology: Topology::new(options.clone())?,
            session_pool: ServerSessionPool::new(),
            options,
            shutdown: Shutdown {
                pending_drops: SyncMutex::new(IdSet::new()),
                executed: AtomicBool::new(false),
            },
            #[cfg(feature = "in-use-encryption-unstable")]
            csfle: Default::default(),
        });
        Ok(Self { inner })
    }

    /// Return an `EncryptedClientBuilder` for constructing a `Client` with auto-encryption enabled.
    ///
    /// ```no_run
    /// # use bson::doc;
    /// # use mongocrypt::ctx::KmsProvider;
    /// # use mongodb::Client;
    /// # use mongodb::error::Result;
    /// # async fn func() -> Result<()> {
    /// # let client_options = todo!();
    /// # let key_vault_namespace = todo!();
    /// # let key_vault_client: Client = todo!();
    /// # let local_key: bson::Binary = todo!();
    /// let encrypted_client = Client::encrypted_builder(
    ///     client_options,
    ///     key_vault_namespace,
    ///     [(KmsProvider::Local, doc! { "key": local_key }, None)],
    /// )?
    /// .key_vault_client(key_vault_client)
    /// .build()
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "in-use-encryption-unstable")]
    pub fn encrypted_builder(
        client_options: ClientOptions,
        key_vault_namespace: crate::Namespace,
        kms_providers: impl IntoIterator<
            Item = (
                mongocrypt::ctx::KmsProvider,
                bson::Document,
                Option<options::TlsOptions>,
            ),
        >,
    ) -> Result<EncryptedClientBuilder> {
        Ok(EncryptedClientBuilder::new(
            client_options,
            csfle::options::AutoEncryptionOptions::new(
                key_vault_namespace,
                csfle::options::KmsProviders::new(kms_providers)?,
            ),
        ))
    }

    #[cfg(all(test, feature = "in-use-encryption-unstable"))]
    pub(crate) async fn mongocryptd_spawned(&self) -> bool {
        self.inner
            .csfle
            .read()
            .await
            .as_ref()
            .map_or(false, |cs| cs.exec().mongocryptd_spawned())
    }

    #[cfg(all(test, feature = "in-use-encryption-unstable"))]
    pub(crate) async fn has_mongocryptd_client(&self) -> bool {
        self.inner
            .csfle
            .read()
            .await
            .as_ref()
            .map_or(false, |cs| cs.exec().has_mongocryptd_client())
    }

    fn test_command_event_channel(&self) -> Option<&options::TestEventSender> {
        #[cfg(test)]
        {
            self.inner
                .options
                .test_options
                .as_ref()
                .and_then(|t| t.async_event_listener.as_ref())
        }
        #[cfg(not(test))]
        {
            None
        }
    }

    #[cfg(not(feature = "tracing-unstable"))]
    pub(crate) async fn emit_command_event(&self, generate_event: impl FnOnce() -> CommandEvent) {
        let handler = self.inner.options.command_event_handler.as_ref();
        let test_channel = self.test_command_event_channel();
        if handler.is_none() && test_channel.is_none() {
            return;
        }

        let event = generate_event();
        if let Some(tx) = test_channel {
            let (msg, ack) = crate::runtime::AcknowledgedMessage::package(event.clone());
            let _ = tx.send(msg).await;
            ack.wait_for_acknowledgment().await;
        }
        if let Some(handler) = handler {
            handle_command_event(handler.as_ref(), event);
        }
    }

    #[cfg(feature = "tracing-unstable")]
    pub(crate) async fn emit_command_event(&self, generate_event: impl FnOnce() -> CommandEvent) {
        let tracing_emitter = if trace_or_log_enabled!(
            target: COMMAND_TRACING_EVENT_TARGET,
            TracingOrLogLevel::Debug
        ) {
            Some(CommandTracingEventEmitter::new(
                self.inner.options.tracing_max_document_length_bytes,
                self.inner.topology.id,
            ))
        } else {
            None
        };
        let apm_event_handler = self.inner.options.command_event_handler.as_ref();
        let test_channel = self.test_command_event_channel();
        if !(tracing_emitter.is_some() || apm_event_handler.is_some() || test_channel.is_some()) {
            return;
        }

        let event = generate_event();
        if let Some(tx) = test_channel {
            let (msg, ack) = crate::runtime::AcknowledgedMessage::package(event.clone());
            let _ = tx.send(msg).await;
            ack.wait_for_acknowledgment().await;
        }
        if let (Some(event_handler), Some(ref tracing_emitter)) =
            (apm_event_handler, &tracing_emitter)
        {
            handle_command_event(event_handler.as_ref(), event.clone());
            handle_command_event(tracing_emitter, event);
        } else if let Some(event_handler) = apm_event_handler {
            handle_command_event(event_handler.as_ref(), event);
        } else if let Some(ref tracing_emitter) = tracing_emitter {
            handle_command_event(tracing_emitter, event);
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

    /// Gets a handle to the default database specified in the `ClientOptions` or MongoDB connection
    /// string used to construct this `Client`.
    ///
    /// If no default database was specified, `None` will be returned.
    pub fn default_database(&self) -> Option<Database> {
        self.inner
            .options
            .default_database
            .as_ref()
            .map(|db_name| self.database(db_name))
    }

    async fn list_databases_common(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
        session: Option<&mut ClientSession>,
    ) -> Result<Vec<DatabaseSpecification>> {
        let op = ListDatabases::new(filter.into(), false, options.into());
        self.execute_operation(op, session).await.and_then(|dbs| {
            dbs.into_iter()
                .map(|db_spec| {
                    bson::from_slice(db_spec.as_bytes()).map_err(crate::error::Error::from)
                })
                .collect()
        })
    }

    /// Gets information about each database present in the cluster the Client is connected to.
    pub async fn list_databases(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
    ) -> Result<Vec<DatabaseSpecification>> {
        self.list_databases_common(filter, options, None).await
    }

    /// Gets information about each database present in the cluster the Client is connected to
    /// using the provided `ClientSession`.
    pub async fn list_databases_with_session(
        &self,
        filter: impl Into<Option<Document>>,
        options: impl Into<Option<ListDatabasesOptions>>,
        session: &mut ClientSession,
    ) -> Result<Vec<DatabaseSpecification>> {
        self.list_databases_common(filter, options, Some(session))
            .await
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
                    let name = doc
                        .get_str("name")
                        .map_err(|_| ErrorKind::InvalidResponse {
                            message: "Expected \"name\" field in server response, but it was not \
                                      found"
                                .to_string(),
                        })?;
                    Ok(name.to_string())
                })
                .collect(),
            Err(e) => Err(e),
        }
    }

    /// Starts a new `ClientSession`.
    pub async fn start_session(
        &self,
        options: impl Into<Option<SessionOptions>>,
    ) -> Result<ClientSession> {
        let options = options.into();
        if let Some(ref options) = options {
            options.validate()?;
        }
        Ok(ClientSession::new(self.clone(), options, false).await)
    }

    /// Starts a new [`ChangeStream`] that receives events for all changes in the cluster. The
    /// stream does not observe changes from system collections or the "config", "local" or
    /// "admin" databases. Note that this method (`watch` on a cluster) is only supported in
    /// MongoDB 4.0 or greater.
    ///
    /// See the documentation [here](https://www.mongodb.com/docs/manual/changeStreams/) on change
    /// streams.
    ///
    /// Change streams require either a "majority" read concern or no read
    /// concern. Anything else will cause a server error.
    ///
    /// Note that using a `$project` stage to remove any of the `_id` `operationType` or `ns` fields
    /// will cause an error. The driver requires these fields to support resumability. For
    /// more information on resumability, see the documentation for
    /// [`ChangeStream`](change_stream/struct.ChangeStream.html)
    ///
    /// If the pipeline alters the structure of the returned events, the parsed type will need to be
    /// changed via [`ChangeStream::with_type`].
    pub async fn watch(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
    ) -> Result<ChangeStream<ChangeStreamEvent<Document>>> {
        let mut options = options.into();
        resolve_options!(self, options, [read_concern, selection_criteria]);
        options
            .get_or_insert_with(Default::default)
            .all_changes_for_cluster = Some(true);
        let target = AggregateTarget::Database("admin".to_string());
        self.execute_watch(pipeline, options, target, None).await
    }

    /// Starts a new [`SessionChangeStream`] that receives events for all changes in the cluster
    /// using the provided [`ClientSession`].  See [`Client::watch`] for more information.
    pub async fn watch_with_session(
        &self,
        pipeline: impl IntoIterator<Item = Document>,
        options: impl Into<Option<ChangeStreamOptions>>,
        session: &mut ClientSession,
    ) -> Result<SessionChangeStream<ChangeStreamEvent<Document>>> {
        let mut options = options.into();
        resolve_read_concern_with_session!(self, options, Some(&mut *session))?;
        resolve_selection_criteria_with_session!(self, options, Some(&mut *session))?;
        options
            .get_or_insert_with(Default::default)
            .all_changes_for_cluster = Some(true);
        let target = AggregateTarget::Database("admin".to_string());
        self.execute_watch_with_session(pipeline, options, target, None, session)
            .await
    }

    pub(crate) fn register_async_drop(&self) -> AsyncDropToken {
        let (cleanup_tx, cleanup_rx) = tokio::sync::oneshot::channel::<BoxFuture<'static, ()>>();
        let (id_tx, id_rx) = tokio::sync::oneshot::channel::<crate::id_set::Id>();
        let weak = self.weak();
        let handle = crate::runtime::spawn(async move {
            // Unwrap safety: the id is sent immediately after task creation, with no
            // await points in between.
            let id = id_rx.await.unwrap();
            // If the cleanup channel is closed, that task was dropped.
            let cleanup = if let Ok(f) = cleanup_rx.await {
                f
            } else {
                return;
            };
            cleanup.await;
            if let Some(client) = weak.upgrade() {
                client
                    .inner
                    .shutdown
                    .pending_drops
                    .lock()
                    .unwrap()
                    .remove(&id);
            }
        });
        let id = self
            .inner
            .shutdown
            .pending_drops
            .lock()
            .unwrap()
            .insert(handle);
        let _ = id_tx.send(id);
        AsyncDropToken {
            tx: Some(cleanup_tx),
        }
    }

    /// Shut down this `Client`, terminating background thread workers and closing connections.
    /// This will wait for any live handles to server-side resources (see below) to be
    /// dropped and any associated server-side operations to finish.
    ///
    /// IMPORTANT: Any live resource handles that are not dropped will cause this method to wait
    /// indefinitely.  It's strongly recommended to structure your usage to avoid this, e.g. by
    /// only using those types in shorter-lived scopes than the `Client`.  If this is not possible,
    /// see [`shutdown_immediate`](Client::shutdown_immediate).  For example:
    ///
    /// ```rust
    /// # use mongodb::{Client, GridFsBucket, error::Result};
    /// async fn upload_data(bucket: &GridFsBucket) {
    ///   let stream = bucket.open_upload_stream("test", None);
    ///    // .. write to the stream ..
    /// }
    ///
    /// # async fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com").await?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// upload_data(&bucket).await;
    /// client.shutdown().await;
    /// // Background cleanup work from `upload_data` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If the handle is used in the same scope as `shutdown`, explicit `drop` may be needed:
    ///
    /// ```rust
    /// # use mongodb::{Client, error::Result};
    /// # async fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com").await?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// let stream = bucket.open_upload_stream("test", None);
    /// // .. write to the stream ..
    /// drop(stream);
    /// client.shutdown().await;
    /// // Background cleanup work for `stream` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Calling any methods on  clones of this `Client` or derived handles after this will return
    /// errors.
    ///
    /// Handles to server-side resources are `Cursor`, `SessionCursor`, `Session`, or
    /// `GridFsUploadStream`.
    pub async fn shutdown(self) {
        // Subtle bug: if this is inlined into the `join_all(..)` call, Rust will extend the
        // lifetime of the temporary unnamed `MutexLock` until the end of the *statement*,
        // causing the lock to be held for the duration of the join, which deadlocks.
        let pending = self.inner.shutdown.pending_drops.lock().unwrap().extract();
        join_all(pending).await;
        self.shutdown_immediate().await;
    }

    /// Shut down this `Client`, terminating background thread workers and closing connections.
    /// This does *not* wait for other pending resources to be cleaned up, which may cause both
    /// client-side errors and server-side resource leaks. Calling any methods on clones of this
    /// `Client` or derived handles after this will return errors.
    ///
    /// ```rust
    /// # use mongodb::{Client, error::Result};
    /// # async fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com").await?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// let stream = bucket.open_upload_stream("test", None);
    /// // .. write to the stream ..
    /// client.shutdown_immediate().await;
    /// // Background cleanup work for `stream` may or may not have run.
    /// # Ok(())
    /// # }
    /// ```
    pub async fn shutdown_immediate(self) {
        self.inner.topology.shutdown().await;
        // This has to happen last to allow pending cleanup to execute commands.
        self.inner.shutdown.executed.store(true, Ordering::SeqCst);
    }

    /// Add connections to the connection pool up to `min_pool_size`.  This is normally not needed -
    /// the connection pool will be filled in the background, and new connections created as needed
    /// up to `max_pool_size`.  However, it can sometimes be preferable to pay the (larger) cost of
    /// creating new connections up-front so that individual operations execute as quickly as
    /// possible.
    ///
    /// Note that topology changes require rebuilding the connection pool, so this method cannot
    /// guarantee that the pool will always be filled for the lifetime of the `Client`.
    ///
    /// Does nothing if `min_pool_size` is unset or zero.
    pub async fn warm_connection_pool(&self) {
        if !self.inner.options.min_pool_size.map_or(false, |s| s > 0) {
            // No-op when min_pool_size is zero.
            return;
        }
        self.inner.topology.warm_pool().await;
    }

    /// Check in a server session to the server session pool. The session will be discarded if it is
    /// expired or dirty.
    pub(crate) async fn check_in_server_session(&self, session: ServerSession) {
        let timeout = self.inner.topology.logical_session_timeout();
        self.inner.session_pool.check_in(session, timeout).await;
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
    ) -> Result<ServerAddress> {
        let server = self.select_server(criteria, "Test select server").await?;
        Ok(server.address.clone())
    }

    /// Select a server using the provided criteria. If none is provided, a primary read preference
    /// will be used instead.
    #[allow(unused_variables)] // we only use the operation_name for tracing.
    async fn select_server(
        &self,
        criteria: Option<&SelectionCriteria>,
        operation_name: &str,
    ) -> Result<SelectedServer> {
        let criteria =
            criteria.unwrap_or(&SelectionCriteria::ReadPreference(ReadPreference::Primary));

        let start_time = Instant::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);

        #[cfg(feature = "tracing-unstable")]
        let event_emitter = ServerSelectionTracingEventEmitter::new(
            self.inner.topology.id,
            criteria,
            operation_name,
            start_time,
            timeout,
        );
        #[cfg(feature = "tracing-unstable")]
        event_emitter.emit_started_event(self.inner.topology.watch().observe_latest().description);
        // We only want to emit this message once per operation at most.
        #[cfg(feature = "tracing-unstable")]
        let mut emitted_waiting_message = false;

        let mut watcher = self.inner.topology.watch();
        loop {
            let state = watcher.observe_latest();

            let result = server_selection::attempt_to_select_server(
                criteria,
                &state.description,
                &state.servers(),
            );
            match result {
                Err(error) => {
                    #[cfg(feature = "tracing-unstable")]
                    event_emitter.emit_failed_event(&state.description, &error);

                    return Err(error);
                }
                Ok(result) => {
                    if let Some(server) = result {
                        #[cfg(feature = "tracing-unstable")]
                        event_emitter.emit_succeeded_event(&state.description, &server);

                        return Ok(server);
                    } else {
                        #[cfg(feature = "tracing-unstable")]
                        if !emitted_waiting_message {
                            event_emitter.emit_waiting_event(&state.description);
                            emitted_waiting_message = true;
                        }

                        watcher.request_immediate_check();

                        let change_occurred = start_time.elapsed() < timeout
                            && watcher
                                .wait_for_update(timeout - start_time.elapsed())
                                .await;
                        if !change_occurred {
                            let error: Error = ErrorKind::ServerSelection {
                                message: state
                                    .description
                                    .server_selection_timeout_error_message(criteria),
                            }
                            .into();

                            #[cfg(feature = "tracing-unstable")]
                            event_emitter.emit_failed_event(&state.description, &error);

                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    #[cfg(all(test, not(feature = "sync"), not(feature = "tokio-sync")))]
    pub(crate) fn get_hosts(&self) -> Vec<String> {
        let watcher = self.inner.topology.watch();
        let state = watcher.peek_latest();

        state
            .servers()
            .keys()
            .map(|stream_address| format!("{}", stream_address))
            .collect()
    }

    #[cfg(test)]
    pub(crate) async fn sync_workers(&self) {
        self.inner.topology.sync_workers().await;
    }

    #[cfg(test)]
    pub(crate) fn topology_description(&self) -> crate::sdam::TopologyDescription {
        self.inner
            .topology
            .watch()
            .peek_latest()
            .description
            .clone()
    }

    #[cfg(test)]
    pub(crate) fn topology(&self) -> &Topology {
        &self.inner.topology
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) async fn primary_description(&self) -> Option<crate::sdam::ServerDescription> {
        let start_time = Instant::now();
        let timeout = self
            .inner
            .options
            .server_selection_timeout
            .unwrap_or(DEFAULT_SERVER_SELECTION_TIMEOUT);
        let mut watcher = self.inner.topology.watch();
        loop {
            let topology = watcher.observe_latest();
            if let Some(desc) = topology.description.primary() {
                return Some(desc.clone());
            }
            if !watcher
                .wait_for_update(timeout - start_time.elapsed())
                .await
            {
                return None;
            }
        }
    }

    pub(crate) fn weak(&self) -> WeakClient {
        WeakClient {
            inner: TrackingArc::downgrade(&self.inner),
        }
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) async fn auto_encryption_opts(
        &self,
    ) -> Option<tokio::sync::RwLockReadGuard<'_, csfle::options::AutoEncryptionOptions>> {
        tokio::sync::RwLockReadGuard::try_map(self.inner.csfle.read().await, |csfle| {
            csfle.as_ref().map(|cs| cs.opts())
        })
        .ok()
    }

    #[cfg(test)]
    pub(crate) fn options(&self) -> &ClientOptions {
        &self.inner.options
    }
}

#[derive(Clone, Debug)]
pub(crate) struct WeakClient {
    inner: crate::tracking_arc::Weak<ClientInner>,
}

impl WeakClient {
    pub(crate) fn upgrade(&self) -> Option<Client> {
        self.inner.upgrade().map(|inner| Client { inner })
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct AsyncDropToken {
    #[derivative(Debug = "ignore")]
    tx: Option<tokio::sync::oneshot::Sender<BoxFuture<'static, ()>>>,
}

impl AsyncDropToken {
    pub(crate) fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(fut.boxed());
        } else {
            #[cfg(debug_assertions)]
            panic!("exhausted AsyncDropToken");
        }
    }

    pub(crate) fn take(&mut self) -> Self {
        Self { tx: self.tx.take() }
    }
}
