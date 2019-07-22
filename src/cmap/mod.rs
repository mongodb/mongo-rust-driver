#[cfg(test)]
mod test;

mod background;
mod conn;
pub(crate) mod options;
mod wait_queue;

use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
    time::{Duration, Instant},
};

use derivative::Derivative;

pub use self::conn::ConnectionInfo;
use self::{conn::Connection, options::ConnectionPoolOptions, wait_queue::WaitQueue};
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler, ConnectionCheckoutFailedEvent, ConnectionCheckoutFailedReason,
        ConnectionCheckoutStartedEvent, ConnectionClosedReason, PoolClearedEvent, PoolClosedEvent,
        PoolCreatedEvent,
    },
};

const DEFAULT_MAX_POOL_SIZE: usize = 100;

// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    // The address of the server the pool's connections will connect to.
    address: String,

    // If a checkout operation takes longer than `wait_queue_timeout`, the pool will return an
    // error. If `wait_queue_timeout` is `None`, then the checkout operation will not time out.
    wait_queue_timeout: Option<Duration>,

    // Connections that have been ready for usage in the pool for longer than `max_idle_time` will
    // be closed either by the background thread or when popped off of the set of available
    // connections. If `max_idle_time` is `None`, then connections will not be closed due to being
    // idle.
    max_idle_time: Option<Duration>,

    // The maximum number of connections that the pool can have at a given time. This includes
    // connections which are currently checked out of the pool.
    max_pool_size: u32,

    // The minimum number of connections that the pool can have at a given time. This includes
    // connections which are currently checked out of the pool.
    min_pool_size: Option<u32>,
    generation: AtomicU32,
    total_connection_count: AtomicU32,
    next_connection_id: AtomicU32,
    wait_queue: WaitQueue,
    connections: Arc<RwLock<Vec<Connection>>>,
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
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);
        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);

        let event_handler = event_handler.map(Arc::from);

        let inner = ConnectionPoolInner {
            address: address.into(),
            wait_queue_timeout,
            max_idle_time,
            max_pool_size,
            min_pool_size,
            generation: AtomicU32::new(0),
            total_connection_count: AtomicU32::new(0),
            next_connection_id: AtomicU32::new(1),
            wait_queue: WaitQueue::new(address.into(), wait_queue_timeout, event_handler.clone()),
            connections: Default::default(),
            event_handler,
        };

        let pool = Self {
            inner: Arc::new(inner),
        };

        pool.emit_event(move |handler| {
            let event = PoolCreatedEvent {
                address: address.into(),
                options,
            };

            handler.handle_pool_created_event(event);
        });

        background::start_background_thread(Arc::downgrade(&pool.inner));

        pool
    }

    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<CmapEventHandler>),
    {
        if let Some(ref handler) = self.inner.event_handler {
            emit(handler);
        }
    }

    pub(crate) fn check_out(&self) -> Result<Connection> {
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let mut conn = self.check_out_helper()?;
        conn.setup();

        self.emit_event(|handler| {
            handler.handle_connection_checked_out_event(conn.checked_out_event());
        });

        Ok(conn)
    }

    fn check_out_helper(&self) -> Result<Connection> {
        let start_time = Instant::now();
        let mut handle = self.inner.wait_queue.wait_until_at_front()?;

        loop {
            // Try to get the most recent available connection.
            while let Some(conn) = self.inner.connections.write().unwrap().pop() {
                // Close the connection if it's stale.
                if conn.is_stale(self.inner.generation.load(Ordering::SeqCst)) {
                    self.close_connection(conn, ConnectionClosedReason::Stale, true);
                    continue;
                }

                // Close the connection if it's idle.
                if conn.is_idle(self.inner.max_idle_time) {
                    self.close_connection(conn, ConnectionClosedReason::Idle, true);
                    continue;
                }

                // Otherwise, return the connection.
                return Ok(conn);
            }

            // Create a new connection if the pool is under max size.
            if self.inner.total_connection_count.load(Ordering::SeqCst) < self.inner.max_pool_size {
                return Ok(self.inner.create_connection());
            }

            // Check if the pool has a max timeout.
            if let Some(timeout) = self.inner.wait_queue_timeout {
                // Check how long since the checkout process began.
                let time_waiting = Instant::now().duration_since(start_time);

                // If the timeout has been reached, return an error.
                if time_waiting >= timeout {
                    self.emit_event(|handler| {
                        let event = ConnectionCheckoutFailedEvent {
                            address: self.inner.address.clone(),
                            reason: ConnectionCheckoutFailedReason::Timeout,
                        };

                        handler.handle_connection_checkout_failed_event(event);
                    });

                    bail!(ErrorKind::WaitQueueTimeoutError(self.inner.address.clone()));
                }

                // Wait until the either the timeout has been reached or a connection is checked
                // into the pool.
                handle.wait(Some(timeout - time_waiting))?;
            } else {
                // Wait until a connection has been returned to the pool.
                handle.wait(None)?;
            }
        }
    }

    pub(crate) fn check_in(&self, mut conn: Connection) {
        self.emit_event(|handler| {
            handler.handle_connection_checked_in_event(conn.checked_in_event());
        });

        // Close the connection if it's stale.
        if conn.is_stale(self.inner.generation.load(Ordering::SeqCst)) {
            self.close_connection(conn, ConnectionClosedReason::Stale, false);

            return;
        }

        conn.mark_as_ready_and_available();
        self.inner.connections.write().unwrap().push(conn);
        self.inner.wait_queue.notify_ready();
    }

    pub(crate) fn clear(&self) {
        self.inner.generation.fetch_add(1, Ordering::SeqCst);

        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    fn close_connection(&self, conn: Connection, reason: ConnectionClosedReason, available: bool) {
        conn.close(reason);

        self.inner
            .total_connection_count
            .fetch_sub(1, Ordering::SeqCst);
    }
}

impl ConnectionPoolInner {
    fn create_connection(&self) -> Connection {
        self.total_connection_count.fetch_add(1, Ordering::SeqCst);

        Connection::new(
            self.next_connection_id.fetch_add(1, Ordering::SeqCst),
            &self.address,
            self.generation.load(Ordering::SeqCst),
            self.event_handler.clone(),
        )
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
