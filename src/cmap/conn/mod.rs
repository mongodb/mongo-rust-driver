mod command;
mod stream_description;
mod wire;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;

use self::wire::Message;
use super::manager::PoolManager;
use crate::{
    cmap::options::{ConnectionOptions, StreamOptions},
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
    runtime::AsyncStream,
};
pub(crate) use command::{Command, CommandResponse};
pub(crate) use stream_description::StreamDescription;
pub(crate) use wire::next_request_id;

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
    pub(crate) generation: u32,

    /// The cached StreamDescription from the connection's handshake.
    pub(super) stream_description: Option<StreamDescription>,

    /// Marks the time when the connection was last checked into the pool. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    /// PoolManager used to check this connection back in when dropped.
    /// None when checked into the pool.
    pub(super) pool_manager: Option<PoolManager>,

    /// Whether or not a command is currently being run on this connection. This is set to `true`
    /// right before sending bytes to the server and set back to `false` once a full response has
    /// been read.
    command_executing: bool,

    /// Whether or not this connection has experienced a network error while reading or writing.
    /// Once the connection has received an error, it should not be used again or checked back
    /// into a pool.
    error: bool,

    stream: AsyncStream,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<dyn CmapEventHandler>>,
}

impl Connection {
    async fn new(
        id: u32,
        address: StreamAddress,
        generation: u32,
        options: Option<ConnectionOptions>,
    ) -> Result<Self> {
        let stream_options = StreamOptions {
            address: address.clone(),
            connect_timeout: options.as_ref().and_then(|opts| opts.connect_timeout),
            tls_options: options.as_ref().and_then(|opts| opts.tls_options.clone()),
        };

        let conn = Self {
            id,
            generation,
            pool_manager: None,
            command_executing: false,
            ready_and_available_time: None,
            stream: AsyncStream::connect(stream_options).await?,
            address,
            handler: options.and_then(|options| options.event_handler),
            stream_description: None,
            error: false,
        };

        Ok(conn)
    }

    /// Constructs and connects a new connection.
    pub(super) async fn connect(pending_connection: PendingConnection) -> Result<Self> {
        Self::new(
            pending_connection.id,
            pending_connection.address.clone(),
            pending_connection.generation,
            pending_connection.options,
        )
        .await
    }

    /// Construct and connect a new connection used for monitoring.
    pub(crate) async fn connect_monitoring(
        address: StreamAddress,
        connect_timeout: Option<Duration>,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        Self::new(
            0,
            address,
            0,
            Some(ConnectionOptions {
                connect_timeout,
                tls_options,
                event_handler: None,
            }),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn new_testing(
        id: u32,
        address: StreamAddress,
        generation: u32,
        options: Option<ConnectionOptions>,
    ) -> Result<Self> {
        Self::new(id, address, generation, options).await
    }

    pub(crate) fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            id: self.id,
            address: self.address.clone(),
        }
    }

    pub(crate) fn address(&self) -> &StreamAddress {
        &self.address
    }

    /// Helper to mark the time that the connection was checked into the pool for the purpose of
    /// detecting when it becomes idle.
    pub(super) fn mark_as_available(&mut self) {
        self.pool_manager.take();
        self.ready_and_available_time = Some(Instant::now());
    }

    /// Helper to mark that the connection has been checked out of the pool. This ensures that the
    /// connection is not marked as idle based on the time that it's checked out and that it has a
    /// reference to the pool.
    pub(super) fn mark_as_in_use(&mut self, manager: PoolManager) {
        self.pool_manager = Some(manager);
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

    /// Checks if the connection is currently executing an operation.
    pub(super) fn is_executing(&self) -> bool {
        self.command_executing
    }

    /// Checks if the connection experienced a network error and should be closed.
    pub(super) fn has_errored(&self) -> bool {
        self.error
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

    /// Helper to create a `ConnectionClosedEvent` for the connection.
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
    pub(crate) async fn send_command(
        &mut self,
        command: Command,
        request_id: impl Into<Option<i32>>,
    ) -> Result<CommandResponse> {
        let message = Message::with_command(command, request_id.into());

        self.command_executing = true;
        let write_result = message.write_to(&mut self.stream).await;
        self.error = write_result.is_err();
        write_result?;

        let response_message_result = Message::read_from(&mut self.stream).await;
        self.command_executing = false;
        self.error = response_message_result.is_err();

        CommandResponse::new(self.address.clone(), response_message_result?)
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

    /// Close this connection, emitting a `ConnectionClosedEvent` with the supplied reason.
    pub(super) fn close_and_drop(mut self, reason: ConnectionClosedReason) {
        self.close(reason);
    }

    /// Close this connection, emitting a `ConnectionClosedEvent` with the supplied reason.
    fn close(&mut self, reason: ConnectionClosedReason) {
        self.pool_manager.take();
        if let Some(ref handler) = self.handler {
            handler.handle_connection_closed_event(self.closed_event(reason));
        }
    }

    /// Nullify the inner state and return it in a new `Connection` for checking back in to
    /// the pool.
    fn take(&mut self) -> Connection {
        Connection {
            id: self.id,
            address: self.address.clone(),
            generation: self.generation,
            stream: std::mem::replace(&mut self.stream, AsyncStream::Null),
            handler: self.handler.take(),
            stream_description: self.stream_description.take(),
            command_executing: self.command_executing,
            error: self.error,
            pool_manager: None,
            ready_and_available_time: None,
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // If the connection has a pool manager, that means that the connection is
        // being dropped when it's checked out. If the pool is still alive, it
        // should check itself back in. Otherwise, the connection should close
        // itself and emit a ConnectionClosed event (because the `close_and_drop`
        // helper was not called explicitly).
        //
        // If the connection does not have a pool manager, then the connection is
        // being dropped while it's not checked out. This means that the pool called
        // the `close_and_drop` helper explicitly, so we don't add it back to the
        // pool or emit any events.
        if let Some(pool_manager) = self.pool_manager.take() {
            let dropped_connection = self.take();
            if let Err(mut conn) = pool_manager.check_in(dropped_connection) {
                // the check in failed because the pool has been dropped, so we emit the event
                // here and drop the connection.
                conn.close(ConnectionClosedReason::PoolClosed);
            }
        }
    }
}

/// Struct encapsulating the information needed to establish a `Connection`.
///
/// Creating a `PendingConnection` contributes towards the total connection count of a pool, despite
/// not actually making a TCP connection to the pool's endpoint. This models a "pending" Connection
/// from the CMAP specification.
#[derive(Debug)]
pub(super) struct PendingConnection {
    pub(super) id: u32,
    pub(super) address: StreamAddress,
    pub(super) generation: u32,
    pub(super) options: Option<ConnectionOptions>,
}

impl PendingConnection {
    /// Helper to create a `ConnectionCreatedEvent` for the connection.
    pub(super) fn created_event(&self) -> ConnectionCreatedEvent {
        ConnectionCreatedEvent {
            address: self.address.clone(),
            connection_id: self.id,
        }
    }
}
