use std::time::{Duration, Instant};

use super::ConnectionPool;
use crate::{
    error::Result,
    event::cmap::{
        ConnectionCheckedInEvent, ConnectionCheckedOutEvent, ConnectionClosedEvent,
        ConnectionClosedReason, ConnectionCreatedEvent, ConnectionReadyEvent,
    },
};

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    pub(super) id: u32,
    pub(super) address: String,
    pub(super) generation: u32,
    established: bool,

    /// Marks the time when the connection was checked into the pool and established. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    /// A reference to the pool that maintains the connection. If the connection is currently
    /// checked into the pool, this will be None (to avoid the pool being kept alive indefinitely
    /// by a reference cycle).
    pub(super) pool: Option<ConnectionPool>,
}

impl Connection {
    /// Constructs a new connection.
    pub(super) fn new(id: u32, address: &str, generation: u32) -> Self {
        Self {
            id,
            address: address.into(),
            generation,
            pool: None,
            established: false,
            ready_and_available_time: None,
        }
    }

    /// Returning a connection to its pool when it's dropped is not achievable directly due to
    /// `drop` only taking a mutable reference to `self` rather than ownership. `duplicate` lets
    /// us get around this by creating a new connection that's identical to the existing one and
    /// adding that one back to the pool (which doesn't conflict with the original connection
    /// due to it being dropped immediately afterwards).
    fn duplicate(&self) -> Self {
        Self {
            id: self.id,
            address: self.address.clone(),
            generation: self.generation,
            pool: None,
            established: self.established,
            ready_and_available_time: self.ready_and_available_time,
        }
    }

    /// Helper to mark the time that the connection was checked into the pool for the purpose of
    /// detecting when it becomes idle.
    pub(super) fn mark_as_ready_and_available(&mut self) {
        self.ready_and_available_time = Some(Instant::now());
    }

    /// Checks if the connection is idle.
    pub(super) fn is_idle(&self, max_idle_time: Option<Duration>) -> bool {
        self.ready_and_available_time
            .and_then(|ready_and_available_time| {
                max_idle_time.map(|max_idle_time| {
                    Instant::now().duration_since(ready_and_available_time) >= max_idle_time
                })
            })
            .unwrap_or(false)
    }

    /// Checks if the connection is stale.
    pub(super) fn is_stale(&self, current_generation: u32) -> bool {
        self.generation != current_generation
    }

    /// Placeholder method that establishes the connection.
    pub(super) fn setup(&mut self) -> Result<()> {
        if self.established {
            return Ok(());
        }

        // TODO: Auth, handshake, etc. No need to implement in this module though.

        self.established = true;
        Ok(())
    }

    /// Helper to create a `ConnectionCheckedOutEvent` for the connection.
    pub(super) fn checked_out_event(&self) -> ConnectionCheckedOutEvent {
        ConnectionCheckedOutEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionCheckedInEvent` for the connection.
    pub(super) fn checked_in_event(&self) -> ConnectionCheckedInEvent {
        ConnectionCheckedInEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn ready_event(&self) -> ConnectionReadyEvent {
        ConnectionReadyEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn created_event(&self) -> ConnectionCreatedEvent {
        ConnectionCreatedEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }

    /// Helper to create a `ConnectionReadyEvent` for the connection.
    pub(super) fn closed_event(&self, reason: ConnectionClosedReason) -> ConnectionClosedEvent {
        ConnectionClosedEvent {
            address: self.address.clone(),
            connection_id: self.id,
            reason,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(ref pool) = self.pool.take() {
            pool.check_in(self.duplicate());
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
