#[cfg(test)]
mod test;

pub(crate) mod options;
mod wait_queue;

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock, Weak,
    },
    time::{Duration, Instant},
};

use derivative::Derivative;

use self::{options::ConnectionPoolOptions, wait_queue::WaitQueue};
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler, ConnectionCheckedInEvent, ConnectionCheckedOutEvent,
        ConnectionCheckoutFailedEvent, ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent, ConnectionClosedEvent, ConnectionClosedReason,
        ConnectionCreatedEvent, ConnectionReadyEvent, PoolClearedEvent, PoolClosedEvent,
        PoolCreatedEvent,
    },
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    pub(crate) id: u32,

    pub(crate) hostname: String,

    pub(crate) generation: u32,

    established: bool,

    ready_and_available_time: Option<Instant>,

    pool: Option<Weak<RwLock<ConnectionPool>>>,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<CmapEventHandler>>,
}

impl Connection {
    fn new(
        id: u32,
        hostname: &str,
        generation: u32,
        handler: Option<Arc<CmapEventHandler>>,
    ) -> Self {
        if let Some(ref handler) = handler {
            let event = ConnectionCreatedEvent {
                address: hostname.into(),
                connection_id: id,
            };

            handler.handle_connection_created_event(event);
        }

        let mut conn = Self {
            id,
            hostname: hostname.into(),
            generation,
            pool: None,
            handler,
            established: false,
            ready_and_available_time: None,
        };

        conn.setup();
        conn
    }

    fn duplicate(&self) -> Self {
        Self {
            id: self.id,
            hostname: self.hostname.clone(),
            generation: self.generation,
            pool: None,
            handler: self.handler.clone(),
            established: self.established,
            ready_and_available_time: self.ready_and_available_time,
        }
    }

    fn emit_closed_event(&self, reason: ConnectionClosedReason) {
        if let Some(ref handler) = self.handler {
            let event = ConnectionClosedEvent {
                address: self.hostname.clone(),
                connection_id: self.id,
                reason,
            };

            handler.handle_connection_closed_event(event);
        }
    }

    fn close(mut self, reason: ConnectionClosedReason) {
        self.pool.take();
        self.emit_closed_event(reason);
    }

    fn setup(&mut self) {
        if self.established {
            return;
        }

        // TODO: Auth, handshake, etc. No need to implement in this module though.

        if let Some(ref handler) = self.handler {
            let event = ConnectionReadyEvent {
                address: self.hostname.clone(),
                connection_id: self.id,
            };

            handler.handle_connection_ready_event(event);
        }

        self.established = true;
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // If the connection has a weak reference to a pool, that means that the connection is being
        // dropped when it's checked out. If the pool is still alive, it should check itself back
        // in. Otherwise, the connection should close itself and emit a ConnectionClosed event
        // (because the `close` helper was not called explicitly).
        //
        // If the connection does not have a weak reference to a pool, then the connection is being
        // dropped while it's not checked out. This means that the pool called the close helper
        // explicitly, so we don't add it back to the pool or emit any events.
        if let Some(ref weak_pool_ref) = self.pool {
            if let Some(strong_pool_ref) = weak_pool_ref.upgrade() {
                strong_pool_ref.write().unwrap().check_in(self.duplicate());
            } else {
                self.emit_closed_event(ConnectionClosedReason::PoolClosed);
            }
        }
    }
}

/// User-facing information about a connection to the database.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// The hostname of the address of the server that the connection is connected to.
    pub hostname: String,

    /// The port of the address of the server that the connection is connected to.
    pub port: Option<u16>,
}

#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

#[derive(Derivative)]
#[derivative(Debug)]
struct ConnectionPoolInner {
    address: String,

    max_pool_size: u32,

    max_idle_time: Option<Duration>,

    wait_queue: WaitQueue,

    generation: AtomicU32,

    total_connection_count: AtomicU32,

    available_connection_count: AtomicU32,

    wait_queue_timeout: Option<Duration>,

    connections: Arc<RwLock<Vec<Connection>>>,

    next_connection_id: AtomicU32,

    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<CmapEventHandler>>,
}

impl ConnectionPool {
    pub(crate) fn new(
        address: &str,
        options: Option<ConnectionPoolOptions>,
        event_handler: Option<Box<CmapEventHandler>>,
    ) -> Self {
        let max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);
        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(100);

        let event_handler = event_handler.map(Arc::from);

        let pool = ConnectionPoolInner {
            connections: Default::default(),
            address: address.into(),
            wait_queue: WaitQueue::new(address.into(), wait_queue_timeout, event_handler.clone()),
            generation: AtomicU32::new(0),
            max_pool_size,
            max_idle_time,
            wait_queue_timeout,
            total_connection_count: AtomicU32::new(0),
            available_connection_count: AtomicU32::new(0),
            next_connection_id: AtomicU32::new(1),
            event_handler,
        };

        if let Some(ref handler) = pool.event_handler {
            handler.handle_pool_created_event(PoolCreatedEvent {
                address: address.into(),
                options,
            });
        }

        Self {
            inner: Arc::new(pool),
        }
    }

    pub(crate) fn check_out(&self) -> Result<Connection> {
        if let Some(ref handler) = self.inner.event_handler {
            let event = ConnectionCheckoutStartedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        }

        let mut conn = self.check_out_helper()?;
        conn.setup();

        if let Some(ref handler) = self.inner.event_handler {
            let event = ConnectionCheckedOutEvent {
                address: conn.hostname.clone(),
                connection_id: conn.id,
            };

            handler.handle_connection_checked_out_event(event);
        }

        Ok(conn)
    }

    fn check_out_helper(&self) -> Result<Connection> {
        let start_time = Instant::now();
        let mut handle = self.inner.wait_queue.wait_until_at_front()?;

        loop {
            if let Some(conn) = self.inner.connections.write().unwrap().pop() {
                if self.connection_is_stale(&conn) {
                    self.close_connection(conn, ConnectionClosedReason::Stale, true);

                    continue;
                }

                if self.connection_is_idle(&conn) {
                    self.close_connection(conn, ConnectionClosedReason::Idle, true);

                    continue;
                }

                return Ok(conn);
            }

            if self.inner.total_connection_count.load(Ordering::SeqCst) < self.inner.max_pool_size {
                return Ok(self.create_connection());
            }

            if let Some(timeout) = self.inner.wait_queue_timeout {
                let time_waiting = Instant::now().duration_since(start_time);

                if time_waiting >= timeout {
                    let event = ConnectionCheckoutFailedEvent {
                        address: self.inner.address.clone(),
                        reason: ConnectionCheckoutFailedReason::Timeout,
                    };

                    if let Some(ref handler) = self.inner.event_handler {
                        handler.handle_connection_checkout_failed_event(event);
                    }

                    bail!(ErrorKind::WaitQueueTimeoutError(self.inner.address.clone()));
                }

                handle.wait(Some(timeout - time_waiting))?;
            } else {
                handle.wait(None)?;
            }
        }
    }

    pub(crate) fn check_in(&self, mut conn: Connection) {
        if let Some(ref handler) = self.inner.event_handler {
            let event = ConnectionCheckedInEvent {
                address: conn.hostname.clone(),
                connection_id: conn.id,
            };

            handler.handle_connection_checked_in_event(event);
        }

        if self.connection_is_stale(&conn) {
            self.close_connection(conn, ConnectionClosedReason::Stale, false);

            return;
        }

        if conn.established {
            conn.ready_and_available_time = Some(Instant::now());
        }

        self.inner.connections.write().unwrap().push(conn);
        self.inner.wait_queue.notify_ready();
    }

    pub(crate) fn clear(&self) {
        self.inner.generation.fetch_add(1, Ordering::SeqCst);

        if let Some(ref handler) = self.inner.event_handler {
            let event = PoolClearedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        }
    }

    fn create_connection(&self) -> Connection {
        self.inner
            .total_connection_count
            .fetch_add(1, Ordering::SeqCst);

        Connection::new(
            self.inner.next_connection_id.fetch_add(1, Ordering::SeqCst),
            &self.inner.address,
            self.inner.generation.load(Ordering::SeqCst),
            self.inner.event_handler.clone(),
        )
    }

    fn close_connection(&self, conn: Connection, reason: ConnectionClosedReason, available: bool) {
        conn.close(reason);

        self.inner
            .total_connection_count
            .fetch_sub(1, Ordering::SeqCst);

        if available {
            self.inner
                .available_connection_count
                .fetch_sub(1, Ordering::SeqCst);
        }
    }

    fn connection_is_stale(&self, conn: &Connection) -> bool {
        self.inner.generation.load(Ordering::SeqCst) != conn.generation
    }

    fn connection_is_idle(&self, conn: &Connection) -> bool {
        conn.ready_and_available_time
            .and_then(|ready_and_available_time| {
                self.inner.max_idle_time.map(|max_idle_time| {
                    Instant::now().duration_since(ready_and_available_time) >= max_idle_time
                })
            })
            .unwrap_or(false)
    }
}

impl Drop for ConnectionPoolInner {
    fn drop(&mut self) {
        for conn in self.connections.write().unwrap().drain(..) {
            conn.close(ConnectionClosedReason::PoolClosed);
        }

        if let Some(ref handler) = self.event_handler {
            handler.handle_pool_closed_event(PoolClosedEvent {
                address: self.address.clone(),
            });
        }
    }
}
