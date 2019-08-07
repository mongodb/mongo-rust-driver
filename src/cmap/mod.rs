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

const DEFAULT_MAX_POOL_SIZE: u32 = 100;

/// A pool of connections implementing the CMAP spec. All state is kept internally in an `Arc`, and
/// internal state that is mutable is additionally wrapped by a lock.
#[derive(Clone, Debug)]
pub(crate) struct ConnectionPool {
    inner: Arc<ConnectionPoolInner>,
}

/// The internal state of a connection pool.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct ConnectionPoolInner {
    /// The address of the server the pool's connections will connect to.
    address: String,

    /// If a checkout operation takes longer than `wait_queue_timeout`, the pool will return an
    /// error. If `wait_queue_timeout` is `None`, then the checkout operation will not time out.
    wait_queue_timeout: Option<Duration>,

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

    /// The current generation of the pool. The generation is incremented whenever the pool is
    /// cleared. Connections belonging to a previous generation are considered stale and will be
    /// closed when checked back in or when popped off of the set of available connections.
    generation: AtomicU32,

    /// The total number of connections currently in the pool. This includes connections which are
    /// currently checked out of the pool.
    total_connection_count: AtomicU32,

    /// The ID of the next connection created by the pool.
    next_connection_id: AtomicU32,

    /// Connections are checked out by concurrent threads on a first-come, first-server basis. This
    /// is enforced by threads entering the wait queue when they first try to check out a
    /// connection and then blocking until they are at the front of the queue.
    wait_queue: WaitQueue,

    /// The set of available connections in the pool. Because the CMAP spec requires that
    /// connections are checked out in a FIFO manner, connections are pushed/popped from the back
    /// of the Vec.
    connections: Arc<RwLock<Vec<Connection>>>,

    /// The event handler specified by the user to process CMAP events.
    #[derivative(Debug = "ignore")]
    event_handler: Option<Arc<dyn CmapEventHandler>>,
}

impl ConnectionPool {
    pub(crate) fn new(
        address: &str,
        options: Option<ConnectionPoolOptions>,
        event_handler: Option<Arc<dyn CmapEventHandler>>,
    ) -> Self {
        // Get the individual options from `options`.
        let max_idle_time = options.as_ref().and_then(|opts| opts.max_idle_time);
        let wait_queue_timeout = options.as_ref().and_then(|opts| opts.wait_queue_timeout);
        let max_pool_size = options
            .as_ref()
            .and_then(|opts| opts.max_pool_size)
            .unwrap_or(DEFAULT_MAX_POOL_SIZE);
        let min_pool_size = options.as_ref().and_then(|opts| opts.min_pool_size);

        let inner = ConnectionPoolInner {
            address: address.into(),
            wait_queue_timeout,
            max_idle_time,
            max_pool_size,
            min_pool_size,
            generation: AtomicU32::new(0),
            total_connection_count: AtomicU32::new(0),
            next_connection_id: AtomicU32::new(1),
            wait_queue: WaitQueue::new(address, wait_queue_timeout, event_handler.clone()),
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

    /// Emits an event from the event handler if one is present, where `emit` is a closure that uses
    /// the event handler.
    fn emit_event<F>(&self, emit: F)
    where
        F: FnOnce(&Arc<dyn CmapEventHandler>),
    {
        if let Some(ref handler) = self.inner.event_handler {
            emit(handler);
        }
    }

    /// Checks out a connection from the pool. This method will block until this thread is at the
    /// front of the wait queue, and then will block again if no available connections are in the
    /// pool and the total number of connections is not less than the max pool size. If the method
    /// blocks for longer than `wait_queue_timeout`, a `WaitQueueTimeoutError` will be returned.
    pub(crate) fn check_out(&self) -> Result<Connection> {
        self.emit_event(|handler| {
            let event = ConnectionCheckoutStartedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_connection_checkout_started_event(event);
        });

        let mut conn = self.acquire_or_create_connection()?;
        conn.setup()?;

        self.emit_event(|handler| {
            handler.handle_connection_checked_out_event(conn.checked_out_event());
        });

        Ok(conn)
    }

    /// Waits for the thread to reach the front of the wait queue, then attempts to check out a
    /// connection.
    fn acquire_or_create_connection(&self) -> Result<Connection> {
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
                return Ok(self.inner.create_connection()?);
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
                handle.wait_for_available_connection(Some(timeout - time_waiting))?;
            } else {
                // Wait until a connection has been returned to the pool.
                handle.wait_for_available_connection(None)?;
            }
        }
    }

    /// Checks a connection back into the pool and notifies the wait queue that a connection is
    /// ready. If the connection is stale, it will be closed instead of being added to the set of
    /// available connections. The time that the connection is checked in will be marked to
    /// facilitate detecting if the connection becomes idle.
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

    /// Increments the generation of the pool. Rather than eagerly removing stale connections from
    /// the pool, they are left for the background thread to clean up.
    pub(crate) fn clear(&self) {
        self.inner.generation.fetch_add(1, Ordering::SeqCst);

        self.emit_event(|handler| {
            let event = PoolClearedEvent {
                address: self.inner.address.clone(),
            };

            handler.handle_pool_cleared_event(event);
        });
    }

    /// Internal helper to close a connection, emit the event for it being closed, and decrement the
    /// total connection count. Any connection being closed by the pool should be closed by using
    /// this method.
    fn close_connection(&self, conn: Connection, reason: ConnectionClosedReason, available: bool) {
        conn.close(reason);

        self.inner
            .total_connection_count
            .fetch_sub(1, Ordering::SeqCst);
    }
}

impl ConnectionPoolInner {
    /// Helper method to create a connection and increment the total connection count. This method
    /// is defined on `ConnectionPoolInner` rather than `ConnectionPool` itself to facilitate
    /// calling it from the background thread.
    fn create_connection(&self) -> Result<Connection> {
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
    /// Automatic cleanup for the connection pool. This is defined on `ConnectionPoolInner` rather
    /// than `ConnectionPool` so that it only gets run once all (non-weak) references to the
    /// `ConnectionPoolInner` are dropped.
    fn drop(&mut self) {
        // Remove and close each connection currently available in the pool.
        for conn in self.connections.write().unwrap().drain(..) {
            conn.close(ConnectionClosedReason::PoolClosed);
        }

        // Emit the pool closed event if a handler is present.
        if let Some(ref handler) = self.event_handler {
            handler.handle_pool_closed_event(PoolClosedEvent {
                address: self.address.clone(),
            });
        }
    }
}
