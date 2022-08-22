mod command;
mod stream_description;
mod wire;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;
use serde::Serialize;
use tokio::{
    io::{BufReader, BufWriter},
    sync::{mpsc, Mutex},
};

use self::wire::{Message, MessageFlags};
use super::manager::PoolManager;
use crate::{
    bson::oid::ObjectId,
    cmap::{
        options::{ConnectionOptions, StreamOptions},
        PoolGeneration,
    },
    compression::Compressor,
    error::{load_balanced_mode_mismatch, Error, ErrorKind, Result},
    event::cmap::{
        CmapEventHandler,
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
    },
    options::{ServerAddress, TlsOptions},
    runtime::AsyncStream,
};
pub(crate) use command::{Command, RawCommand, RawCommandResponse};
pub(crate) use stream_description::StreamDescription;
pub(crate) use wire::next_request_id;

/// User-facing information about a connection to the database.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// A server-generated identifier that uniquely identifies the connection. Available on server
    /// versions 4.2+. This may be used to correlate driver connections with server logs.
    pub server_id: Option<i32>,

    /// The address that the connection is connected to.
    pub address: ServerAddress,
}

/// A wrapper around Stream that contains all the CMAP information needed to maintain a connection.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    /// Driver-generated ID for the connection.
    pub(super) id: u32,
    /// Server-generated ID for the connection.
    pub(crate) server_id: Option<i32>,

    pub(crate) address: ServerAddress,
    pub(crate) generation: ConnectionGeneration,

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

    /// Whether the most recently received message included the moreToCome flag, indicating the
    /// server may send more responses without any additional requests. Attempting to send new
    /// messages on this connection while this value is true will return an error. This value
    /// will remain true until a server response does not include the moreToComeFlag.
    more_to_come: bool,

    stream: BufReader<BufWriter<AsyncStream>>,

    /// Compressor that the client will use before sending messages.
    /// This compressor does not get used to decompress server messages.
    /// The client will decompress server messages using whichever compressor
    /// the server indicates in its message.  This compressor is the first
    /// compressor in the client's compressor list that also appears in the
    /// server's compressor list.
    pub(super) compressor: Option<Compressor>,

    /// If the connection is pinned to a cursor or transaction, the channel sender to return this
    /// connection to the pin holder.
    pinned_sender: Option<mpsc::Sender<Connection>>,

    #[derivative(Debug = "ignore")]
    handler: Option<Arc<dyn CmapEventHandler>>,
}

impl Connection {
    async fn new(
        id: u32,
        address: ServerAddress,
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
            server_id: None,
            generation: ConnectionGeneration::Normal(generation),
            pool_manager: None,
            command_executing: false,
            ready_and_available_time: None,
            stream: BufReader::new(BufWriter::new(AsyncStream::connect(stream_options).await?)),
            address,
            handler: options.and_then(|options| options.event_handler),
            stream_description: None,
            error: false,
            pinned_sender: None,
            compressor: None,
            more_to_come: false,
        };

        Ok(conn)
    }

    /// Constructs and connects a new connection.
    pub(super) async fn connect(pending_connection: PendingConnection) -> Result<Self> {
        let generation = match pending_connection.generation {
            PoolGeneration::Normal(gen) => gen,
            PoolGeneration::LoadBalanced(_) => 0, /* Placeholder; will be overwritten in
                                                   * `ConnectionEstablisher::
                                                   * establish_connection`. */
        };
        Self::new(
            pending_connection.id,
            pending_connection.address.clone(),
            generation,
            pending_connection.options,
        )
        .await
    }

    /// Construct and connect a new connection used for monitoring.
    pub(crate) async fn connect_monitoring(
        address: ServerAddress,
        tls_options: Option<TlsOptions>,
    ) -> Result<Self> {
        Self::new(
            0,
            address,
            0,
            Some(ConnectionOptions {
                connect_timeout: None, // handled by the monitor
                tls_options,
                event_handler: None,
            }),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn new_testing(
        id: u32,
        address: ServerAddress,
        generation: u32,
        options: Option<ConnectionOptions>,
    ) -> Result<Self> {
        Self::new(id, address, generation, options).await
    }

    pub(crate) fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            id: self.id,
            server_id: self.server_id,
            address: self.address.clone(),
        }
    }

    pub(crate) fn service_id(&self) -> Option<ObjectId> {
        self.generation.service_id()
    }

    pub(crate) fn address(&self) -> &ServerAddress {
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

    async fn send_message(
        &mut self,
        message: Message,
        to_compress: bool,
    ) -> Result<RawCommandResponse> {
        if self.more_to_come {
            return Err(Error::internal(format!(
                "attempted to send a new message to {} but moreToCome bit was set",
                self.address()
            )));
        }

        self.command_executing = true;

        // If the client has agreed on a compressor with the server, and the command
        // is the right type of command, then compress the message.
        let write_result = match self.compressor {
            Some(ref compressor) if to_compress => {
                message
                    .write_compressed_to(&mut self.stream, compressor)
                    .await
            }
            _ => message.write_to(&mut self.stream).await,
        };

        self.error = write_result.is_err();
        write_result?;

        let response_message_result = Message::read_from(
            &mut self.stream,
            self.stream_description
                .as_ref()
                .map(|d| d.max_message_size_bytes),
        )
        .await;
        self.command_executing = false;
        self.error = response_message_result.is_err();

        let response_message = response_message_result?;
        self.more_to_come = response_message.flags.contains(MessageFlags::MORE_TO_COME);

        RawCommandResponse::new(self.address.clone(), response_message)
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
    ) -> Result<RawCommandResponse> {
        let to_compress = command.should_compress();
        let message = Message::with_command(command, request_id.into())?;
        self.send_message(message, to_compress).await
    }

    /// Executes a `RawCommand` and returns a `CommandResponse` containing the result from the
    /// server.
    ///
    /// An `Ok(...)` result simply means the server received the command and that the driver
    /// received the response; it does not imply anything about the success of the command
    /// itself.
    pub(crate) async fn send_raw_command(
        &mut self,
        command: RawCommand,
        request_id: impl Into<Option<i32>>,
    ) -> Result<RawCommandResponse> {
        let to_compress = command.should_compress();
        let message = Message::with_raw_command(command, request_id.into());
        self.send_message(message, to_compress).await
    }

    /// Receive the next message from the connection.
    /// This will return an error if the previous response on this connection did not include the
    /// moreToCome flag.
    pub(crate) async fn receive_message(&mut self) -> Result<RawCommandResponse> {
        if !self.more_to_come {
            return Err(Error::internal(format!(
                "attempted to stream response from connection to {} but moreToCome bit was not set",
                self.address()
            )));
        }

        self.command_executing = true;
        let response_message_result = Message::read_from(
            &mut self.stream,
            self.stream_description
                .as_ref()
                .map(|d| d.max_message_size_bytes),
        )
        .await;
        self.command_executing = false;
        self.error = response_message_result.is_err();

        let response_message = response_message_result?;
        self.more_to_come = response_message.flags.contains(MessageFlags::MORE_TO_COME);

        RawCommandResponse::new(self.address.clone(), response_message)
    }

    /// Gets the connection's StreamDescription.
    pub(crate) fn stream_description(&self) -> Result<&StreamDescription> {
        self.stream_description.as_ref().ok_or_else(|| {
            ErrorKind::Internal {
                message: "Stream checked out but not handshaked".to_string(),
            }
            .into()
        })
    }

    /// Pin the connection, removing it from the normal connection pool.
    pub(crate) fn pin(&mut self) -> Result<PinnedConnectionHandle> {
        if self.pinned_sender.is_some() {
            return Err(Error::internal(format!(
                "cannot pin an already-pinned connection (id = {})",
                self.id
            )));
        }
        if self.pool_manager.is_none() {
            return Err(Error::internal(format!(
                "cannot pin a checked-in connection (id = {})",
                self.id
            )));
        }
        let (tx, rx) = mpsc::channel(1);
        self.pinned_sender = Some(tx);
        Ok(PinnedConnectionHandle {
            id: self.id,
            receiver: Arc::new(Mutex::new(rx)),
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
            server_id: self.server_id,
            address: self.address.clone(),
            generation: self.generation.clone(),
            stream: std::mem::replace(
                &mut self.stream,
                BufReader::new(BufWriter::new(AsyncStream::Null)),
            ),
            handler: self.handler.take(),
            stream_description: self.stream_description.take(),
            command_executing: self.command_executing,
            error: self.error,
            pool_manager: None,
            ready_and_available_time: None,
            pinned_sender: self.pinned_sender.clone(),
            compressor: self.compressor.clone(),
            more_to_come: false,
        }
    }

    /// Whether or not the previous command response indicated that the server may send
    /// more responses without another request.
    pub(crate) fn is_streaming(&self) -> bool {
        self.more_to_come
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
            let mut dropped_connection = self.take();
            let result = if let Some(sender) = self.pinned_sender.as_mut() {
                // Preserve the pool manager and timestamp for pinned connections.
                dropped_connection.pool_manager = Some(pool_manager.clone());
                dropped_connection.ready_and_available_time = self.ready_and_available_time;
                match sender.try_send(dropped_connection) {
                    Ok(()) => Ok(()),
                    // The connection has been unpinned and should be checked back in.
                    Err(mpsc::error::TrySendError::Closed(mut conn)) => {
                        conn.pinned_sender = None;
                        conn.ready_and_available_time = None;
                        pool_manager.check_in(conn)
                    }
                    // The connection is being returned to the pin holder while another connection
                    // is in the pin buffer; this should never happen.  Only possible action is to
                    // check the connection back in.
                    Err(mpsc::error::TrySendError::Full(mut conn)) => {
                        // Panic in debug mode
                        if cfg!(debug_assertions) {
                            panic!(
                                "buffer full when attempting to return a pinned connection (id = \
                                 {})",
                                conn.id
                            );
                        }
                        // TODO RUST-230 log an error in non-debug mode.
                        conn.pinned_sender = None;
                        conn.ready_and_available_time = None;
                        pool_manager.check_in(conn)
                    }
                }
            } else {
                pool_manager.check_in(dropped_connection)
            };
            if let Err(mut conn) = result {
                // the check in failed because the pool has been dropped, so we emit the event
                // here and drop the connection.
                conn.close(ConnectionClosedReason::PoolClosed);
            }
        }
    }
}

/// A handle to a pinned connection - the connection itself can be retrieved or returned to the
/// normal pool via this handle.
#[derive(Debug)]
pub(crate) struct PinnedConnectionHandle {
    id: u32,
    receiver: Arc<Mutex<mpsc::Receiver<Connection>>>,
}

impl PinnedConnectionHandle {
    /// Make a new `PinnedConnectionHandle` that refers to the same connection as this one.
    /// Use with care and only when "lending" a handle in a way that can't be expressed as a
    /// normal borrow.
    pub(crate) fn replicate(&self) -> Self {
        Self {
            id: self.id,
            receiver: self.receiver.clone(),
        }
    }

    /// Retrieve the pinned connection, blocking until it's available for use.  Will fail if the
    /// connection has been unpinned.
    pub(crate) async fn take_connection(&self) -> Result<Connection> {
        let mut receiver = self.receiver.lock().await;
        receiver.recv().await.ok_or_else(|| {
            Error::internal(format!(
                "cannot take connection after unpin (id={})",
                self.id
            ))
        })
    }

    pub(crate) fn id(&self) -> u32 {
        self.id
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ConnectionGeneration {
    Normal(u32),
    LoadBalanced {
        generation: u32,
        service_id: ObjectId,
    },
}

impl ConnectionGeneration {
    pub(crate) fn service_id(&self) -> Option<ObjectId> {
        match self {
            ConnectionGeneration::Normal(_) => None,
            ConnectionGeneration::LoadBalanced { service_id, .. } => Some(*service_id),
        }
    }

    pub(crate) fn is_stale(&self, current_generation: &PoolGeneration) -> bool {
        match (self, current_generation) {
            (ConnectionGeneration::Normal(cgen), PoolGeneration::Normal(pgen)) => cgen != pgen,
            (
                ConnectionGeneration::LoadBalanced {
                    generation: cgen,
                    service_id,
                },
                PoolGeneration::LoadBalanced(gen_map),
            ) => cgen != gen_map.get(service_id).unwrap_or(&0),
            _ => load_balanced_mode_mismatch!(false),
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
    pub(super) address: ServerAddress,
    pub(super) generation: PoolGeneration,
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
