mod command;
mod stream;
mod stream_description;
mod wire;

use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use derivative::Derivative;

use self::{stream::Stream, wire::Message};
use super::ConnectionPoolInner;
use crate::{
    error::{ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
    },
    options::{StreamAddress, TlsOptions},
};
pub(crate) use command::{Command, CommandResponse};
pub(crate) use stream_description::StreamDescription;

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

    /// The cached StreamDescription from the connection's handshake.
    pub(super) stream_description: Option<StreamDescription>,

    /// Marks the time when the connection was checked into the pool and established. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    /// The connection will have a weak reference to its pool when it's checked out. When it's
    /// A reference to the pool that maintains the connection. If the connection is currently
    /// currently checked into the pool, this will be None.
    pub(super) pool: Option<Weak<ConnectionPoolInner>>,

    stream: Stream,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<dyn CmapEventHandler>>,
}

impl Connection {
    /// Constructs a new connection.
    pub(crate) fn new(
        id: u32,
        address: StreamAddress,
        generation: u32,
        tls_options: Option<TlsOptions>,
        handler: Option<Arc<dyn CmapEventHandler>>,
    ) -> Result<Self> {
        let conn = Self {
            id,
            generation,
            pool: None,
            stream_description: None,
            ready_and_available_time: None,
            stream: Stream::connect(address.clone(), tls_options)?,
            address,
            handler,
        };

        Ok(conn)
    }

    pub(crate) fn address(&self) -> &StreamAddress {
        &self.address
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
            stream_description: Some(Default::default()),
            ready_and_available_time: None,
            stream: Stream::Null,
            handler: None,
        }
    }

    /// Helper to mark the time that the connection was checked into the pool for the purpose of
    /// detecting when it becomes idle.
    pub(super) fn mark_checked_in(&mut self) {
        self.pool.take();
        self.ready_and_available_time = Some(Instant::now());
    }

    /// Helper to mark that the connection has been checked out of the pool. This ensures that the
    /// connection is not marked as idle based on the time that it's checked out and that it has a
    /// reference to the pool.
    pub(super) fn mark_checked_out(&mut self, pool: Weak<ConnectionPoolInner>) {
        self.pool = Some(pool);
        self.ready_and_available_time.take();
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
        CommandResponse::new(self.address.clone(), response_message)
    }

    /// Gets the connection's StreamDescription.
    pub(crate) fn stream_description(&self) -> Result<&StreamDescription> {
        self.stream_description.as_ref().ok_or_else(|| {
            ErrorKind::OperationError {
                message: "Stream checked out but not handshaked".to_string(),
            }
            .into()
        })
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
                strong_pool_ref.check_in(std::mem::replace(self, Self::null()));
            } else if let Some(ref handler) = self.handler {
                handler.handle_connection_closed_event(
                    self.closed_event(ConnectionClosedReason::PoolClosed),
                );
            }
        }
    }
}
