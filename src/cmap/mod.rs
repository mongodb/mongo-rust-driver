#[cfg(test)]
mod test;

mod background;
pub(crate) mod conn;
mod establish;
pub(crate) mod options;
mod wait_queue;

use std::{sync::Arc, time::Duration};

use derivative::Derivative;
use tokio::sync::Mutex;

pub use self::conn::ConnectionInfo;
pub(crate) use self::conn::{Command, CommandResponse, Connection, StreamDescription};
use self::{
    establish::ConnectionEstablisher,
    options::{ConnectionOptions, ConnectionPoolOptions},
    wait_queue::WaitQueue,
};
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckoutFailedEvent,
        ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent,
        ConnectionClosedReason,
        PoolClearedEvent,
        PoolClosedEvent,
        PoolCreatedEvent,
    },
    options::StreamAddress,
    runtime::HttpClient,
    RUNTIME,
};

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

impl From<Arc<ConnectionPoolInner>> for ConnectionPool {
    fn from(inner: Arc<ConnectionPoolInner>) -> Self {
        Self { inner }
    }
}

/// A struct used to manage the creation, closing, and storage of connections for a
/// `ConnectionPool`.
#[derive(Debug)]
struct ConnectionManager {
    /// The set of available connections in the pool. Because the CMAP spec requires that
    /// connections are checked out in a FIFO manner, connections are pushed/popped from the back
    /// of the Vec.
    checked_in_connections: Vec<Connection>,

    /// The total number of connections managed by the pool, including connections which are
    /// currently checked out of the pool.
    total_connection_count: u32,

    /// The ID of the next connection created by the pool.
    next_connection_id: u32,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: u32,

    /// The address to create connections to.
    address: StreamAddress,

    /// The options used to create connections.
    connection_options: Option<ConnectionOptions>,

    /// Contains the logic for "establishing" a connection. This includes handshaking and
    /// authenticating a connection when it's first created.
    establisher: ConnectionEstablisher,
}

impl ConnectionManager {
    fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> Self {
        let connection_options: Option<ConnectionOptions> = options
            .as_ref()
            .map(|pool_options| ConnectionOptions::from(pool_options.clone()));

        Self {
            checked_in_connections: Vec::new(),
            total_connection_count: 0,
            next_connection_id: 1,
            generation: 0,
            establisher: ConnectionEstablisher::new(http_client, options.as_ref()),
            address,
            connection_options,
        }
    }

    /// Emits an event from the event handler if one is present, where `emit` is a closure that uses
    /// the event handler.
    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(handler) = self
            .connection_options
            .as_ref()
            .and_then(|options| options.event_handler.as_ref())
        {
            emit(handler);
        }
    }

    /// Fetches the next connection id, incrementing it for the next connection.
    fn next_connection_id(&mut self) -> u32 {
        let id = self.next_connection_id;
        self.next_connection_id += 1;
        id
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    fn clear(&mut self) {
        self.generation += 1;

        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    /// Create a connection, incrementing the total connection count and emitting the appropriate
    /// monitoring events.
    async fn create_connection(&mut self) -> Result<Connection> {
        let mut connection = Connection::new(
            self.next_connection_id(),
            self.address.clone(),
            self.generation,
            self.connection_options.clone(),
        )
        .await?;

        self.emit_event(|handler| {
            handler.handle_connection_created_event(connection.created_event())
        });

        let establish_result = self.establisher.establish_connection(&mut connection).await;

        if let Err(e) = establish_result {
            if e.is_authentication_error() {
                // auth spec requires that the pool be cleared when encountering an auth error
                // during establishment.
                self.clear();
            }
            return Err(e);
        }

        self.total_connection_count += 1;
        self.emit_event(|handler| handler.handle_connection_ready_event(connection.ready_event()));

        Ok(connection)
    }

    /// Close a connection, emit the event for it being closed, and decrement the
    /// total connection count.
    fn close_connection(&mut self, connection: Connection, reason: ConnectionClosedReason) {
        connection.close_and_drop(reason);
        self.total_connection_count -= 1;
    }
}

/// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// The structure used to manage connection creation, closing, storage, and generation.
    connection_manager: Arc<Mutex<ConnectionManager>>,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,

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

    /// The queue that operations wait in to check out a connection.
    ///
    /// A thread will only reach the front of the queue if a connection is available to be checked
    /// out or if one could be created without going over `max_pool_size`.
    wait_queue: WaitQueue,
}

impl ConnectionPool {
    pub(crate) fn new(
        address: StreamAddress,
        http_client: HttpClient,
        options: Option<ConnectionPoolOptions>,
    ) -> Self {
        let connection_manager =
            ConnectionManager::new(address.clone(), http_client, options.clone());

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
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        let inner = ConnectionPoolInner {
            address: address.clone(),
            event_handler,
            max_idle_time,
            min_pool_size,
            connection_manager: Arc::new(Mutex::new(connection_manager)),
            wait_queue: WaitQueue::new(address.clone(), max_pool_size, wait_queue_timeout),
        };

        let pool = Self {
            inner: Arc::new(inner),
        };

        pool.inner.emit_event(move |handler| {
            let event = PoolCreatedEvent { address, options };
            handler.handle_pool_created_event(event);
        });

        background::start_background_task(Arc::downgrade(&pool.inner));

        pool
    }

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout`, a `WaitQueueTimeoutError` will be returned.
    pub(crate) async fn check_out(&self) -> Result<Connection> {
        let mut conn = self.inner.check_out().await?;
        conn.mark_checked_out(Arc::downgrade(&self.inner));
        Ok(conn)
    }

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
    #[cfg(test)]
    pub(crate) async fn check_in(&self, conn: Connection) {
        self.inner.check_in(conn).await;
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) async fn clear(&self) {
        self.inner.clear().await;
    }
}

impl ConnectionPoolInner {
    /// Emits an event from the event handler if one is present, where `emit` is a closure that uses
    /// the event handler.
    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.event_handler {
            emit(handler);
        }
    }

    async fn check_in(&self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        conn.mark_checked_in();

        let mut connection_manager = self.connection_manager.lock().await;

        // Close the connection if it's stale.
        if conn.is_stale(connection_manager.generation) {
            connection_manager.close_connection(conn, ConnectionClosedReason::Stale);
        } else {
            connection_manager.checked_in_connections.push(conn);
        }

        self.wait_queue.wake_front();
    }

    async fn check_out(&self) -> Result<Connection> {
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let result = self.acquire_or_create_connection().await;

        let conn = match result {
            Ok(conn) => conn,
            Err(e) => {
                let failure_reason =
                    if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                        ConnectionCheckoutFailedReason::Timeout
                    } else {
                        ConnectionCheckoutFailedReason::ConnectionError
                    };

                self.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(ConnectionCheckoutFailedEvent {
                        address: self.address.clone(),
                        reason: failure_reason,
                    })
                });

                return Err(e);
            }
        };

        self.emit_event(|handler| {
            handler.handle_connection_checked_out_event(conn.checked_out_event());
        });

        Ok(conn)
    }

    /// Waits for the thread to reach the front of the wait queue, then attempts to check out a
    /// connection. If none are available in the pool, one is created and checked out instead.
    async fn acquire_or_create_connection(&self) -> Result<Connection> {
        // Handle that will wake up the front of the queue when dropped.
        // Before returning a valid connection, this handle must be disarmed to prevent the front
        // from waking up early.
        let mut wait_queue_handle = self.wait_queue.wait_until_at_front().await?;

        // Try to get the most recent available connection.
        let mut connection_manager = self.connection_manager.lock().await;
        while let Some(conn) = connection_manager.checked_in_connections.pop() {
            // Close the connection if it's stale.
            if conn.is_stale(connection_manager.generation) {
                connection_manager.close_connection(conn, ConnectionClosedReason::Stale);
                continue;
            }

            // Close the connection if it's idle.
            if conn.is_idle(self.max_idle_time) {
                connection_manager.close_connection(conn, ConnectionClosedReason::Idle);
                continue;
            }

            // Otherwise, return the connection.
            wait_queue_handle.disarm();
            return Ok(conn);
        }

        // There are no connections in the pool, so open a new one.
        let connection = connection_manager.create_connection().await?;
        wait_queue_handle.disarm();
        Ok(connection)
    }

    async fn clear(&self) {
        self.connection_manager.lock().await.clear();
    }
}

impl Drop for ConnectionPoolInner {
    /// Automatic cleanup for the connection pool. This is defined on `ConnectionPoolInner` rather
    /// than `ConnectionPool` so that it only gets run once all (non-weak) references to the
    /// `ConnectionPoolInner` are dropped.
    fn drop(&mut self) {
        let address = self.address.clone();
        let connection_manager = self.connection_manager.clone();
        let event_handler = self.event_handler.clone();

        RUNTIME.execute(async move {
            let mut connection_manager = connection_manager.lock().await;
            while let Some(connection) = connection_manager.checked_in_connections.pop() {
                connection_manager.close_connection(connection, ConnectionClosedReason::PoolClosed);
            }

            if let Some(ref handler) = event_handler {
                handler.handle_pool_closed_event(PoolClosedEvent {
                    address: address.clone(),
                });
            }
        });
    }
}
