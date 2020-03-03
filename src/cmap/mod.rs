#[cfg(test)]
mod test;

mod background;
pub(crate) mod conn;
mod establish;
pub(crate) mod options;
mod wait_queue;

use std::{
    sync::Arc,
    time::Duration,
};

use derivative::Derivative;
use tokio::sync::{Mutex, RwLock};

pub use self::conn::ConnectionInfo;
pub(crate) use self::conn::{Command, CommandResponse, Connection, StreamDescription};

use self::{
    establish::ConnectionEstablisher,
    options::ConnectionPoolOptions,
    wait_queue::WaitQueue,
};
use crate::{
    client::auth::Credential,
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
    RUNTIME,
    options::{StreamAddress, TlsOptions},
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

/// A struct containing the checked in connections as well as a count of all connections that
/// belong to the pool, including those that have been checked out already.
#[derive(Debug)]
struct Connections {
    /// The set of available connections in the pool. Because the CMAP spec requires that
    /// connections are checked out in a FIFO manner, connections are pushed/popped from the back
    /// of the Vec.
    checked_in_connections: Vec<Connection>,

    /// The total number of connections currently in the pool. This includes connections which are
    /// currently checked out of the pool.
    total_connection_count: u32,
}

impl Default for Connections {
    fn default() -> Self {
        Self {
            checked_in_connections: Vec::new(),
            total_connection_count: 0,
        }
    }
}

/// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    /// The address the pool's connections will connect to.
    address: StreamAddress,

    /// The set of available connections in the pool. Because the CMAP spec requires that
    /// connections are checked out in a FIFO manner, connections are pushed/popped from the back
    /// of the Vec.
    connections: Arc<Mutex<Connections>>,

    /// The connect timeout passed to each underlying TcpStream when attemtping to connect to the
    /// server.
    connect_timeout: Option<Duration>,

    /// The credential to use for authenticating connections in this pool.
    credential: Option<Credential>,

    /// Contains the logic for "establishing" a connection. This includes handshaking and
    /// authenticating a connection when it's first created.
    establisher: ConnectionEstablisher,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: RwLock<u32>,

    /// Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    /// be closed either by the background thread or when popped off of the set of available
    /// connections. If `max_idle_time` is `None`, then connections will not be closed due to being
    /// idle.
    max_idle_time: Option<Duration>,

    /// The maximum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool.
    max_pool_size: u32,

    /// The minimum number of connections that the pool can have at a given time. This includes
    /// connections which are currently checked out of the pool. If fewer than `min_pool_size`
    /// connections are in the pool, the background thread will create more connections and add
    /// them to the pool.
    min_pool_size: Option<u32>,

    /// The ID of the next connection created by the pool.
    next_connection_id: Mutex<u32>,
    
    /// The TLS options to use for the connections. If `tls_options` is None, then TLS will not be
    /// used to connect to the server.
    tls_options: Option<TlsOptions>,
    
    /// The queue that operations wait in to check out a connection.
    wait_queue: WaitQueue,
}

impl ConnectionPool {
    pub(crate) fn new(address: StreamAddress, mut options: Option<ConnectionPoolOptions>) -> Self {
        // Get the individual options from `options`.
        let connect_timeout = options.as_ref().and_then(|opts| opts.connect_timeout);
        let credential = options.as_mut().and_then(|opts| opts.credential.clone());
        let establisher = ConnectionEstablisher::new(options.as_ref());
        let event_handler = options.as_mut().and_then(|opts| opts.event_handler.take());

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
        let tls_options = options.as_mut().and_then(|opts| opts.tls_options.take());
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);

        let inner = ConnectionPoolInner {
            address: address.clone(),
            connect_timeout,
            credential,
            establisher,
            event_handler,
            generation: RwLock::new(0),
            max_idle_time,
            max_pool_size,
            min_pool_size,
            next_connection_id: Mutex::new(1),
            tls_options,
            connections: Default::default(),
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
        self.inner.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let result = self.acquire_or_create_connection().await;
        
        let mut conn = match result {
            Ok(conn) => conn,
            Err(e) => {
                let failure_reason = if let ErrorKind::WaitQueueTimeoutError { .. } = e.kind.as_ref() {
                    ConnectionCheckoutFailedReason::Timeout
                } else {
                    ConnectionCheckoutFailedReason::ConnectionError
                };

                self.inner.emit_event(|handler| {
                    handler.handle_connection_checkout_failed_event(
                        ConnectionCheckoutFailedEvent {
                            address: self.inner.address.clone(),
                            reason: failure_reason,
                        },
                    )
                });
                
                return Err(e);
            }
        };

        self.inner.emit_event(|handler| {
            handler.handle_connection_checked_out_event(conn.checked_out_event());
        });

        conn.mark_checked_out(Arc::downgrade(&self.inner));
        
        Ok(conn)
    }

    /// Waits for the thread to reach the front of the wait queue, then attempts to check out a
    /// connection. If none are available in the pool, one is created and checked out instead.
    async fn acquire_or_create_connection(&self) -> Result<Connection> {
        self.inner.wait_queue.wait_until_at_front().await?;
        
        // Try to get the most recent available connection.
        let mut connections = self.inner.connections.lock().await;
        while let Some(conn) = connections.checked_in_connections.pop() {
            // Close the connection if it's stale.
            if conn.is_stale(*self.inner.generation.read().await) {
                self.inner.close_connection(conn, ConnectionClosedReason::Stale).await;
                continue;
            }

            // Close the connection if it's idle.
            if conn.is_idle(self.inner.max_idle_time) {
                self.inner.close_connection(conn, ConnectionClosedReason::Idle).await;
                continue;
            }

            // Otherwise, return the connection.
            return Ok(conn);
        }

        // There are no connections in the pool, so create a new one.
        self.create_connection(&mut connections).await
    }

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
    #[cfg(test)]
    pub(crate) fn check_in(&self, conn: Connection) {
        self.inner.check_in(conn);
    }

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) async fn clear(&self) {
        *self.inner.generation.write().await += 1;

        self.inner.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    async fn next_connection_id(&self) -> u32 {
        let mut id_lock = self.inner.next_connection_id.lock().await;
        let id = *id_lock;
        *id_lock += 1;
        id
    }
    
    /// Helper method to create a connection, incrementing the total connection count and emitting the appropriate
    /// monitoring events.
    async fn create_connection(&self, connections: &mut Connections) -> Result<Connection> {
        let mut connection = Connection::new(
            self.next_connection_id().await,
            self.inner.address.clone(),
            *self.inner.generation.read().await,
            self.inner.connect_timeout,
            self.inner.tls_options.clone(),
            self.inner.event_handler.clone(),
        )?;

        self.inner.emit_event(|handler| {
            handler.handle_connection_created_event(connection.created_event())
        });

        let establish_result = self
            .inner
            .establisher
            .establish_connection(&mut connection, self.inner.credential.as_ref());

        if let Err(e) = establish_result {
            if e.is_authentication_error() {
                // auth spec requires that the pool be cleared when encountering an auth error during establishment.
                self.clear().await;
            }
            return Err(e);
        }

        connections.total_connection_count += 1;
        self.inner.emit_event(|handler| handler.handle_connection_ready_event(connection.ready_event()));
        
        Ok(connection)
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

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
    async fn check_in(&self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        conn.mark_checked_in();

        // Close the connection if it's stale.
        if conn.is_stale(*self.generation.read().await) {
            self.close_connection(conn, ConnectionClosedReason::Stale).await;
        } else {
            self.connections.lock().await.checked_in_connections.push(conn);
        }

        self.wait_queue.wake_front();
    }

    /// Internal helper to close a connection, emit the event for it being closed, and decrement the
    /// total connection count. Any connection being closed by the pool should be closed by using
    /// this method.
   async fn close_connection(&self, conn: Connection, reason: ConnectionClosedReason) {
        self.emit_event(|handler| {
            handler.handle_connection_closed_event(conn.closed_event(reason));
        });

        self.connections.lock().await.total_connection_count -= 1;
    }
}

impl Drop for ConnectionPoolInner {
    /// Automatic cleanup for the connection pool. This is defined on `ConnectionPoolInner` rather
    /// than `ConnectionPool` so that it only gets run once all (non-weak) references to the
    /// `ConnectionPoolInner` are dropped.
    fn drop(&mut self) {
        let address = self.address.clone();
        let connections = self.connections.clone();
        let event_handler = self.event_handler.clone();

        RUNTIME.execute(async move {
            for mut conn in connections.lock().await.checked_in_connections.drain(..) {
                conn.pool.take();
                
                if let Some(ref handler) = event_handler {
                    handler.handle_connection_closed_event(
                        conn.closed_event(
                            ConnectionClosedReason::PoolClosed
                        ),
                    );
                }
            }
            
            if let Some(ref handler) = event_handler {
                handler.handle_pool_closed_event(PoolClosedEvent {
                    address: address.clone(),
                });
            }
        });
    }
}
