use std::{
    sync::{Arc, RwLock, Weak},
    time::{Duration, Instant},
};

use super::ConnectionPool;
use crate::event::cmap::{
    CmapEventHandler, ConnectionCheckedInEvent, ConnectionCheckedOutEvent, ConnectionClosedEvent,
    ConnectionClosedReason, ConnectionCreatedEvent, ConnectionReadyEvent,
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    pub(crate) id: u32,
    pub(crate) address: String,
    pub(crate) generation: u32,
    established: bool,

    // Marks the time when the connection was checked into the pool and established. This is used
    // to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    // The connection will have a weak reference to its pool when it's checked out. When it's
    // currently checked into the pool, this will be None.
    pool: Option<Weak<RwLock<ConnectionPool>>>,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<CmapEventHandler>>,
}

impl Connection {
    pub(crate) fn new(
        id: u32,
        address: &str,
        generation: u32,
        handler: Option<Arc<CmapEventHandler>>,
    ) -> Self {
        if let Some(ref handler) = handler {
            let event = ConnectionCreatedEvent {
                address: address.into(),
                connection_id: id,
            };

            handler.handle_connection_created_event(event);
        }

        let mut conn = Self {
            id,
            address: address.into(),
            generation,
            pool: None,
            handler,
            established: false,
            ready_and_available_time: None,
        };

        conn.setup();
        conn
    }

    // Returning a connection to its pool when it's dropped is not achievable directly due to `drop`
    // only taking a mutable reference to `self` rather than ownership. `duplicate` lets us get
    // around this by creating a new connection that's identical to the existing one and adding
    // that one back to the pool (which doesn't conflict with the original connection due to it
    // being dropped immediately afterwards).
    fn duplicate(&self) -> Self {
        Self {
            id: self.id,
            address: self.address.clone(),
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
                address: self.address.clone(),
                connection_id: self.id,
                reason,
            };

            handler.handle_connection_closed_event(event);
        }
    }

    pub(crate) fn mark_as_ready_and_available(&mut self) {
        self.ready_and_available_time = Some(Instant::now());
    }

    pub(crate) fn is_idle(&self, max_idle_time: Option<Duration>) -> bool {
        self.ready_and_available_time
            .and_then(|ready_and_available_time| {
                max_idle_time.map(|max_idle_time| {
                    Instant::now().duration_since(ready_and_available_time) >= max_idle_time
                })
            })
            .unwrap_or(false)
    }

    pub(crate) fn is_stale(&self, current_generation: u32) -> bool {
        self.generation != current_generation
    }

    // Helper to close the connection and emit the corresponding CMAP event. This will only be
    // called by ConnectionPool.
    pub(crate) fn close(mut self, reason: ConnectionClosedReason) {
        self.pool.take();
        self.emit_closed_event(reason);
    }

    // Placeholder method that establishes the connection.
    pub(crate) fn setup(&mut self) {
        if self.established {
            return;
        }

        // TODO: Auth, handshake, etc. No need to implement in this module though.

        if let Some(ref handler) = self.handler {
            let event = ConnectionReadyEvent {
                address: self.address.clone(),
                connection_id: self.id,
            };

            handler.handle_connection_ready_event(event);
        }

        self.established = true;
    }

    pub(crate) fn checked_out_event(&self) -> ConnectionCheckedOutEvent {
        ConnectionCheckedOutEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    pub(crate) fn checked_in_event(&self) -> ConnectionCheckedInEvent {
        ConnectionCheckedInEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
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
