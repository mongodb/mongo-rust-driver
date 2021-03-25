use derivative::Derivative;

use super::{
    conn::PendingConnection,
    connection_requester,
    connection_requester::{
        ConnectionRequest,
        ConnectionRequestReceiver,
        ConnectionRequestResult,
        ConnectionRequester,
    },
    establish::ConnectionEstablisher,
    manager,
    manager::{ManagementRequestReceiver, PoolManagementRequest, PoolManager},
    options::{ConnectionOptions, ConnectionPoolOptions},
    status,
    status::{PoolGenerationPublisher, PoolGenerationSubscriber},
    Connection,
    DEFAULT_MAX_POOL_SIZE,
};
use crate::{
    error::Result,
    event::cmap::{
        CmapEventHandler,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolReadyEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
    sdam::ServerUpdateSender,
    RUNTIME,
};

use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const MAX_CONNECTING: u32 = 2;
const MAINTENACE_FREQUENCY: Duration = Duration::from_millis(500);

/// A worker task that manages the shared state of the pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolWorker {
    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// Current state of the pool. Determines if connections may be checked out
    /// and if min_pool_size connection creation should continue.
    state: PoolState,

    /// The total number of connections managed by the pool, including connections which are
    /// currently checked out of the pool or have yet to be established.
    total_connection_count: u32,

    /// The number of connections currently being established by this pool.
    pending_connection_count: u32,

    /// The ID of the next connection created by the pool.
    next_connection_id: u32,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: u32,

    /// The established connections that are currently checked into the pool and awaiting usage in
    /// future operations.
    available_connections: VecDeque<Connection>,

    /// Contains the logic for "establishing" a connection. This includes handshaking and
    /// authenticating a connection when it's first created.
    establisher: ConnectionEstablisher,

    /// The options used to create new connections.
    connection_options: Option<ConnectionOptions>,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,

    /// The time between maintenance tasks.
    maintenance_frequency: Duration,

    /// Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    /// be closed either by the background thread or when popped off of the set of available
    /// connections. If `max_idle_time` is `None`, then connections will not be closed due to being
    /// idle.
    max_idle_time: Option<Duration>,

    /// The minimum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool. If fewer than `min_pool_size`
    /// connections are in the pool, the background thread will create more connections and add
    /// them to the pool.
    min_pool_size: Option<u32>,

    /// The maximum number of connections that the pool can manage, including connections checked
    /// out of the pool. If a thread requests a connection and the pool is empty + there are
    /// already max_pool_size connections in use, it will block until one is returned or the
    /// wait_queue_timeout is exceeded.
    max_pool_size: u32,

    /// Receiver used to determine if any threads hold references to this pool. If all the
    /// sender ends of this receiver drop, this worker will be notified and drop too.
    handle_listener: HandleListener,

    /// Receiver for incoming connection check out requests.
    request_receiver: ConnectionRequestReceiver,

    /// Ordered queue of incoming requests waiting for connections.
    wait_queue: VecDeque<ConnectionRequest>,

    /// Receiver for incoming pool management requests (e.g. checking in a connection).
    management_receiver: ManagementRequestReceiver,

    /// Sender used to publish the latest generation and notifications for any establishment errors
    /// encountered by the pool.
    generation_publisher: PoolGenerationPublisher,

    /// A pool manager that can be cloned and attached to connections checked out of the pool.
    manager: PoolManager,

    /// A handle used to notify SDAM that a connection establishment error happened. This will
    /// allow the server to transition to Unknown and clear the pool as necessary.
    server_updater: ServerUpdateSender,
}

impl ConnectionPoolWorker {
    /// Starts a worker and returns a manager and connection requester.
    /// Once all connection requesters are dropped, the worker will stop executing
    /// and close the pool.
    pub(super) fn start(
        address: StreamAddress,
        http_client: HttpClient,
        server_updater: ServerUpdateSender,
        options: Option<ConnectionPoolOptions>,
    ) -> (PoolManager, ConnectionRequester, PoolGenerationSubscriber) {
        let establisher = ConnectionEstablisher::new(http_client, options.as_ref());
        let event_handler = options.as_ref().and_then(|opts| opts.event_handler.clone());

        // The CMAP spec indicates that a max idle time of zero means that connections should not be
        // closed due to idleness.
        let mut max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        if max_idle_time == Some(Duration::from_millis(0)) {
            max_idle_time = None;
        }

        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);

        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);

        let connection_options: Option<ConnectionOptions> = options
            .as_ref()
            .map(|pool_options| ConnectionOptions::from(pool_options.clone()));

        let (handle, handle_listener) = handle_channel();
        let (connection_requester, request_receiver) =
            connection_requester::channel(address.clone(), handle);
        let (manager, management_receiver) = manager::channel();
        let (generation_publisher, generation_subscriber) = status::channel();

        #[cfg(test)]
        let state = if options
            .as_ref()
            .and_then(|opts| opts.ready)
            .unwrap_or(false)
        {
            PoolState::Ready
        } else {
            PoolState::Paused
        };
        #[cfg(test)]
        let maintenance_frequency = options
            .as_ref()
            .and_then(|opts| opts.maintenance_frequency)
            .unwrap_or(MAINTENACE_FREQUENCY);

        #[cfg(not(test))]
        let (state, maintenance_frequency) = (PoolState::Paused, MAINTENACE_FREQUENCY);

        let worker = ConnectionPoolWorker {
            address,
            event_handler: event_handler.clone(),
            max_idle_time,
            min_pool_size,
            establisher,
            next_connection_id: 1,
            total_connection_count: 0,
            pending_connection_count: 0,
            generation: 0,
            connection_options,
            available_connections: VecDeque::new(),
            max_pool_size,
            request_receiver,
            wait_queue: Default::default(),
            management_receiver,
            manager: manager.clone(),
            handle_listener,
            state,
            generation_publisher,
            maintenance_frequency,
            server_updater,
        };

        RUNTIME.execute(async move {
            worker.execute().await;
        });

        (manager, connection_requester, generation_subscriber)
    }

    /// Run the worker thread, listening on the various receivers until all handles have been
    /// dropped. Once all handles are dropped, the pool will close any available connections and
    /// emit a pool closed event.
    async fn execute(mut self) {
        let mut maintenance_interval = RUNTIME.interval(self.maintenance_frequency);

        loop {
            let task = tokio::select! {
                Some(request) = self.request_receiver.recv() => {
                    PoolTask::CheckOut(request)
                },
                Some(request) = self.management_receiver.recv() => request.into(),
                _ = self.handle_listener.wait_for_all_handle_drops() => {
                    // all worker handles have been dropped meaning this
                    // pool has no more references and can be dropped itself.
                    break
                },
                _ = maintenance_interval.tick() => {
                    PoolTask::Maintenance
                },
                else => {
                    break
                }
            };

            match task {
                PoolTask::CheckOut(request) => match self.state {
                    PoolState::Ready => {
                        self.wait_queue.push_back(request);
                    }
                    PoolState::Paused => {
                        // if receiver doesn't listen to error that's ok.
                        let _ = request.fulfill(ConnectionRequestResult::PoolCleared);
                    }
                },
                PoolTask::HandleManagementRequest(PoolManagementRequest::CheckIn(connection)) => {
                    self.check_in(connection)
                }
                PoolTask::HandleManagementRequest(PoolManagementRequest::Clear(_message)) => {
                    self.clear();
                }
                PoolTask::HandleManagementRequest(PoolManagementRequest::MarkAsReady {
                    completion_handler: _handler,
                }) => {
                    self.mark_as_ready();
                }
                PoolTask::HandleManagementRequest(
                    PoolManagementRequest::HandleConnectionSucceeded(c),
                ) => self.handle_connection_succeeded(c),
                PoolTask::HandleManagementRequest(
                    PoolManagementRequest::HandleConnectionFailed,
                ) => self.handle_connection_failed(),
                PoolTask::Maintenance => self.perform_maintenance(),
            }

            if self.can_service_connection_request() {
                if let Some(request) = self.wait_queue.pop_front() {
                    self.check_out(request).await;
                }
            }
        }

        while let Some(connection) = self.available_connections.pop_front() {
            connection.close_and_drop(ConnectionClosedReason::PoolClosed);
        }

        self.emit_event(|handler| {
            handler.handle_pool_closed_event(PoolClosedEvent {
                address: self.address.clone(),
            });
        });
    }

    fn can_service_connection_request(&self) -> bool {
        if !matches!(self.state, PoolState::Ready) {
            return false;
        }

        if !self.available_connections.is_empty() {
            return true;
        }

        self.total_connection_count < self.max_pool_size
            && self.pending_connection_count < MAX_CONNECTING
    }

    async fn check_out(&mut self, request: ConnectionRequest) {
        // first attempt to check out an available connection
        while let Some(mut conn) = self.available_connections.pop_back() {
            // Close the connection if it's stale.
            if conn.is_stale(self.generation) {
                self.close_connection(conn, ConnectionClosedReason::Stale);
                continue;
            }

            // Close the connection if it's idle.
            if conn.is_idle(self.max_idle_time) {
                self.close_connection(conn, ConnectionClosedReason::Idle);
                continue;
            }

            conn.mark_as_in_use(self.manager.clone());
            if let Err(request) = request.fulfill(ConnectionRequestResult::Pooled(conn)) {
                // checking out thread stopped listening, indicating it hit the WaitQueue
                // timeout, so we put connection back into pool.
                let mut connection = request.unwrap_pooled_connection();
                connection.mark_as_available();
                self.available_connections.push_back(connection);
            }

            return;
        }

        // otherwise, attempt to create a connection.
        if self.total_connection_count < self.max_pool_size {
            let event_handler = self.event_handler.clone();
            let establisher = self.establisher.clone();
            let pending_connection = self.create_pending_connection();
            let manager = self.manager.clone();
            let mut server_updater = self.server_updater.clone();

            let handle = RUNTIME.spawn(async move {
                let mut establish_result = establish_connection(
                    &establisher,
                    pending_connection,
                    &mut server_updater,
                    &manager,
                    event_handler.as_ref(),
                )
                .await;

                if let Ok(ref mut c) = establish_result {
                    c.mark_as_in_use(manager.clone());
                    manager.handle_connection_succeeded(None);
                }

                establish_result
            });

            let handle = match handle {
                Some(h) => h,

                // The async runtime was dropped which means nothing will be waiting
                // on the request, so we can just exit.
                None => {
                    return;
                }
            };

            // this only fails if the other end stopped listening (e.g. due to timeout), in
            // which case we just let the connection establish in the background.
            let _: std::result::Result<_, _> =
                request.fulfill(ConnectionRequestResult::Establishing(handle));
        } else {
            // put the request to the the front of the wait queue so that it will be processed
            // next time a request can be processed.
            self.wait_queue.push_front(request);
        }
    }

    fn create_pending_connection(&mut self) -> PendingConnection {
        self.total_connection_count += 1;
        self.pending_connection_count += 1;

        let pending_connection = PendingConnection {
            id: self.next_connection_id,
            address: self.address.clone(),
            generation: self.generation,
            options: self.connection_options.clone(),
        };
        self.next_connection_id += 1;
        self.emit_event(|handler| {
            handler.handle_connection_created_event(pending_connection.created_event())
        });

        pending_connection
    }

    /// Process a connection establishment failure.
    fn handle_connection_failed(&mut self) {
        // Establishing a pending connection failed, so that must be reflected in to total
        // connection count.
        self.total_connection_count -= 1;
        self.pending_connection_count -= 1;
    }

    /// Process a successful connection establishment, optionally populating the pool with the
    /// resulting connection.
    fn handle_connection_succeeded(&mut self, connection: Option<Connection>) {
        self.pending_connection_count -= 1;
        if let Some(mut connection) = connection {
            connection.mark_as_available();
            self.available_connections.push_back(connection);
        }
    }

    fn check_in(&mut self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        conn.mark_as_available();

        if conn.has_errored() {
            self.close_connection(conn, ConnectionClosedReason::Error);
        } else if conn.is_stale(self.generation) {
            self.close_connection(conn, ConnectionClosedReason::Stale);
        } else if conn.is_executing() {
            self.close_connection(conn, ConnectionClosedReason::Dropped)
        } else {
            self.available_connections.push_back(conn);
        }
    }

    fn clear(&mut self) {
        self.generation += 1;
        let previous_state = std::mem::replace(&mut self.state, PoolState::Paused);
        self.generation_publisher.publish(self.generation);

        if !matches!(previous_state, PoolState::Paused) {
            self.emit_event(|handler| {
                let event = PoolClearedEvent {
                    address: self.address.clone(),
                };

                handler.handle_pool_cleared_event(event);
            });

            for request in self.wait_queue.drain(..) {
                // an error means the other end hung up already, which is okay because we were
                // returning an error anyways
                let _: std::result::Result<_, _> =
                    request.fulfill(ConnectionRequestResult::PoolCleared);
            }
        }
    }

    fn mark_as_ready(&mut self) {
        if matches!(self.state, PoolState::Ready) {
            return;
        }

        self.state = PoolState::Ready;
        self.emit_event(|handler| {
            let event = PoolReadyEvent {
                address: self.address.clone(),
            };

            handler.handle_pool_ready_event(event);
        });
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    /// Close a connection, emit the event for it being closed, and decrement the
    /// total connection count.
    fn close_connection(&mut self, connection: Connection, reason: ConnectionClosedReason) {
        connection.close_and_drop(reason);
        self.total_connection_count -= 1;
    }

    /// Ensure all connections in the pool are valid and that the pool is managing at least
    /// min_pool_size connections.
    fn perform_maintenance(&mut self) {
        self.remove_perished_connections();
        if matches!(self.state, PoolState::Ready) {
            self.ensure_min_connections();
        }
    }

    /// Iterate over the connections and remove any that are stale or idle.
    fn remove_perished_connections(&mut self) {
        while let Some(connection) = self.available_connections.pop_front() {
            if connection.is_stale(self.generation) {
                // the following unwrap is okay becaue we asserted the pool was nonempty
                self.close_connection(connection, ConnectionClosedReason::Stale);
            } else if connection.is_idle(self.max_idle_time) {
                self.close_connection(connection, ConnectionClosedReason::Idle);
            } else {
                self.available_connections.push_front(connection);
                // All subsequent connections are either not idle or not stale since they were
                // checked into the pool later, so we can just quit early.
                break;
            };
        }
    }

    /// Populate the the pool with enough connections to meet the min_pool_size_requirement.
    fn ensure_min_connections(&mut self) {
        if let Some(min_pool_size) = self.min_pool_size {
            while self.total_connection_count < min_pool_size
                && self.pending_connection_count < MAX_CONNECTING
            {
                let pending_connection = self.create_pending_connection();
                let event_handler = self.event_handler.clone();
                let manager = self.manager.clone();
                let establisher = self.establisher.clone();
                let mut updater = self.server_updater.clone();
                RUNTIME.execute(async move {
                    let connection = establish_connection(
                        &establisher,
                        pending_connection,
                        &mut updater,
                        &manager,
                        event_handler.as_ref(),
                    )
                    .await;

                    if let Ok(connection) = connection {
                        manager.handle_connection_succeeded(Some(connection))
                    }
                });
            }
        }
    }
}

/// Helper covering the common connection establishment behavior between
/// connections established in check_out and those established as part of
/// satisfying min_pool_size.
async fn establish_connection(
    establisher: &ConnectionEstablisher,
    pending_connection: PendingConnection,
    server_updater: &mut ServerUpdateSender,
    manager: &PoolManager,
    event_handler: Option<&Arc<dyn CmapEventHandler>>,
) -> Result<Connection> {
    let connection_id = pending_connection.id;
    let generation = pending_connection.generation;
    let address = pending_connection.address.clone();

    let mut establish_result = establisher.establish_connection(pending_connection).await;

    match establish_result {
        Err(ref e) => {
            if let Some(handler) = event_handler {
                let event = ConnectionClosedEvent {
                    address,
                    reason: ConnectionClosedReason::Error,
                    connection_id,
                };
                handler.handle_connection_closed_event(event);
            }
            server_updater.handle_error(e.clone(), generation).await;
            manager.handle_connection_failed();
        }
        Ok(ref mut connection) => {
            if let Some(handler) = event_handler {
                handler.handle_connection_ready_event(connection.ready_event())
            };
        }
    }

    establish_result
}

/// Enum modeling the possible pool states as described in the CMAP spec.
///
/// The "closed" state is omitted here because the pool considered closed only
/// once it goes out of scope and cannot be manually closed before then.
#[derive(Debug)]
enum PoolState {
    /// Connections may not be checked out nor created in the background to satisfy minPoolSize.
    Paused,

    /// Pool is operational.
    Ready,
}

/// Task to process by the worker.
#[derive(Debug)]
enum PoolTask {
    /// Handle a management request from a `PoolManager`.
    HandleManagementRequest(PoolManagementRequest),

    /// Fulfill the given connection request.
    CheckOut(ConnectionRequest),

    /// Perform pool maintenance (ensure min connections, remove stale or idle connections).
    Maintenance,
}

impl From<PoolManagementRequest> for PoolTask {
    fn from(request: PoolManagementRequest) -> Self {
        PoolTask::HandleManagementRequest(request)
    }
}

/// Constructs a new channel for for monitoring whether this pool still has references
/// to it.
fn handle_channel() -> (PoolWorkerHandle, HandleListener) {
    let (sender, receiver) = mpsc::channel(1);
    (PoolWorkerHandle { sender }, HandleListener { receiver })
}

/// Handle to the worker. Once all handles have been dropped, the worker
/// will stop waiting for new requests and drop the pool itself.
#[derive(Debug, Clone)]
pub(super) struct PoolWorkerHandle {
    sender: mpsc::Sender<()>,
}

impl PoolWorkerHandle {
    #[cfg(test)]
    pub(super) fn new_mocked() -> Self {
        let (s, _) = handle_channel();
        s
    }
}

/// Listener used to determine when all handles have been dropped.
#[derive(Debug)]
struct HandleListener {
    receiver: mpsc::Receiver<()>,
}

impl HandleListener {
    /// Listen until all handles are dropped.
    /// This will not return until all handles are dropped, so make sure to only poll this via
    /// select or with a timeout.
    async fn wait_for_all_handle_drops(&mut self) {
        self.receiver.recv().await;
    }
}
