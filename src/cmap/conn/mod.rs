pub(crate) mod command;
mod stream;
mod wire;

use std::time::{Duration, Instant};

use self::{
    command::{Command, CommandResponse},
    stream::Stream,
    wire::Message,
};
use super::ConnectionPool;
use crate::{
    error::Result,
    event::cmap::{
        ConnectionCheckedInEvent, ConnectionCheckedOutEvent, ConnectionClosedEvent,
        ConnectionClosedReason, ConnectionCreatedEvent, ConnectionReadyEvent,
    },
    options::StreamAddress,
    options::TlsOptions,
};

/// User-facing information about a connection to the database.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// The address that the connection is connected to.
    pub address: StreamAddress,
}

/// A wrapper around Stream that contains all the CMAP information needed to maintain a connection.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    pub(super) id: u32,
    pub(super) address: StreamAddress,
    pub(super) generation: u32,
    established: bool,

    /// Marks the time when the connection was checked into the pool and established. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    /// A reference to the pool that maintains the connection. If the connection is currently
    /// checked into the pool, this will be None (to avoid the pool being kept alive indefinitely
    /// by a reference cycle).
    pub(super) pool: Option<ConnectionPool>,

    stream: Stream,
}

impl Connection {
    /// Constructs a new connection.
    pub(super) fn new(
        id: u32,
        address: StreamAddress,
        generation: u32,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        let conn = Self {
            id,
            generation,
            pool: None,
            established: false,
            ready_and_available_time: None,
            stream: Stream::connect(address.clone(), tls_options)?,
            address,
        };

        Ok(conn)
    }

    /// In order to check a connection back into the pool when it's dropped, we need to be able to
    /// replace it with something. The `null` method facilitates this by creating a dummy connection
    /// which can be passed to `std::mem::replace` to be dropped in place of the original
    /// connection.
    fn null() -> Self {
        Self {
            id: 0,
            address: StreamAddress {
                hostname: Default::default(),
                port: None,
            },
            generation: 0,
            pool: None,
            established: true,
            ready_and_available_time: None,
            stream: Stream::Null,
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

    /// Executes a `Command` and returns a `CommandResponse` containing the result from the server.
    ///
    /// An `Ok(...)` result simply means the server received the command and that the driver
    /// driver received the response; it does not imply anything about the success of the command
    /// itself.
    pub(crate) fn send_command(&mut self, command: Command) -> Result<CommandResponse> {
        let message = Message::from_command(command);
        message.write_to(&mut self.stream)?;

        let response_message = Message::read_from(&mut self.stream)?;
        CommandResponse::from_message(response_message)
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.check_in(std::mem::replace(self, Self::null()));
        }
    }
}
