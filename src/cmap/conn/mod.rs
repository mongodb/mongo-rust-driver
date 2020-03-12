mod command;
mod stream_description;
mod wire;

use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use derivative::Derivative;

use self::wire::Message;
use super::ConnectionPoolInner;
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
    RUNTIME,
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
    pub(super) generation: u32,

    /// The cached StreamDescription from the connection's handshake.
    pub(super) stream_description: Option<StreamDescription>,

    /// Marks the time when the connection was last checked into the pool. This is used
    /// to detect if the connection is idle.
    ready_and_available_time: Option<Instant>,

    /// The connection will have a weak reference to its pool when it's checked out. When it's
    /// A reference to the pool that maintains the connection. If the connection is currently
    /// currently checked into the pool, this will be None.
    pub(super) pool: Option<Weak<ConnectionPoolInner>>,

    stream: AsyncStream,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<dyn CmapEventHandler>>,
}

impl Connection {
    /// Constructs a new connection.
    pub(crate) async fn new(
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
            pool: None,
            ready_and_available_time: None,
            stream: AsyncStream::connect(stream_options).await?,
            address,
            handler: options.and_then(|options| options.event_handler),
            stream_description: None,
        };

        Ok(conn)
    }

    pub(crate) async fn new_monitoring(
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
    pub(crate) async fn send_command(
        &mut self,
        command: Command,
        request_id: impl Into<Option<i32>>,
    ) -> Result<CommandResponse> {
        let message = Message::with_command(command, request_id.into());
        message.write_to(&mut self.stream).await?;

        let response_message = Message::read_from(&mut self.stream).await?;
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

    /// Close this connection, emitting a `ConnectionClosedEvent` with the supplied reason.
    pub(super) fn close_and_drop(mut self, reason: ConnectionClosedReason) {
        self.close(reason);
    }

    /// Close this connection, emitting a `ConnectionClosedEvent` with the supplied reason.
    fn close(&mut self, reason: ConnectionClosedReason) {
        self.pool.take();
        if let Some(ref handler) = self.handler {
            handler.handle_connection_closed_event(self.closed_event(reason));
        }
    }

    /// Nullify the inner state and return it in a `DroppedConnectionState` for checking back in to
    /// the pool in a background task.
    fn take(&mut self) -> DroppedConnectionState {
        self.pool.take();
        DroppedConnectionState {
            id: self.id,
            address: self.address.clone(),
            generation: self.generation,
            stream: std::mem::replace(&mut self.stream, AsyncStream::Null),
            handler: self.handler.take(),
            stream_description: self.stream_description.take(),
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        // If the connection has a weak reference to a pool, that means that the connection is being
        // dropped when it's checked out. If the pool is still alive, it should check itself back
        // in. Otherwise, the connection should close itself and emit a ConnectionClosed event
        // (because the `close_and_drop` helper was not called explicitly).
        //
        // If the connection does not have a weak reference to a pool, then the connection is being
        // dropped while it's not checked out. This means that the pool called the `close_and_drop`
        // helper explicitly, so we don't add it back to the pool or emit any events.
        if let Some(ref weak_pool_ref) = self.pool {
            if let Some(strong_pool_ref) = weak_pool_ref.upgrade() {
                let dropped_connection_state = self.take();
                RUNTIME.execute(async move {
                    strong_pool_ref
                        .check_in(dropped_connection_state.into())
                        .await;
                });
            } else {
                self.close(ConnectionClosedReason::PoolClosed);
            }
        }
    }
}

/// Struct encapsulating the state of a connection that has been dropped.
///
/// Because `Drop::drop` cannot be async, we package the internal state of a dropped connection into
/// this struct, and move it into an async task that will attempt to check the reconstructed
/// connection back into the pool.
///
/// If the async runtime has been dropped, that task will not execute, and this state will be
/// dropped. From there, we simply emit a connection closed event and do not attempt to reconstruct
/// the `Connection`.
#[derive(Derivative)]
#[derivative(Debug)]
struct DroppedConnectionState {
    pub(super) id: u32,
    pub(super) address: StreamAddress,
    pub(super) generation: u32,
    stream: AsyncStream,
    #[derivative(Debug = "ignore")]
    handler: Option<Arc<dyn CmapEventHandler>>,
    stream_description: Option<StreamDescription>,
}

impl Drop for DroppedConnectionState {
    /// If this drop is called, that means the async runtime itself was dropped before the
    /// connection could be checked back in to the pool. Instead of checking back into the pool
    /// again, we just close the connection directly.
    fn drop(&mut self) {
        if let Some(ref handler) = self.handler {
            handler.handle_connection_closed_event(ConnectionClosedEvent {
                address: self.address.clone(),
                connection_id: self.id,
                reason: ConnectionClosedReason::PoolClosed,
            });
        }
    }
}

impl From<DroppedConnectionState> for Connection {
    fn from(mut state: DroppedConnectionState) -> Self {
        Self {
            id: state.id,
            address: state.address.clone(),
            generation: state.generation,
            stream: std::mem::replace(&mut state.stream, AsyncStream::Null),
            handler: state.handler.take(),
            stream_description: state.stream_description.take(),
            ready_and_available_time: None,
            pool: None,
        }
    }
}
