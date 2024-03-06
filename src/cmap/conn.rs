mod command;
mod stream_description;
pub(crate) mod wire;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use derivative::Derivative;
use serde::Serialize;
use tokio::{
    io::BufStream,
    sync::{mpsc, Mutex},
};

use self::wire::{Message, MessageFlags};
use super::manager::PoolManager;
use crate::{
    bson::oid::ObjectId,
    cmap::PoolGeneration,
    compression::Compressor,
    error::{load_balanced_mode_mismatch, Error, ErrorKind, Result},
    event::cmap::{
        CmapEventEmitter,
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionCreatedEvent,
        ConnectionReadyEvent,
    },
    options::ServerAddress,
    runtime::AsyncStream,
};
pub(crate) use command::{Command, RawCommandResponse};
pub(crate) use stream_description::StreamDescription;

/// User-facing information about a connection to the database.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ConnectionInfo {
    /// A driver-generated identifier that uniquely identifies the connection.
    pub id: u32,

    /// A server-generated identifier that uniquely identifies the connection. Available on server
    /// versions 4.2+. This may be used to correlate driver connections with server logs.
    pub server_id: Option<i64>,

    /// The address that the connection is connected to.
    pub address: ServerAddress,
}

/// A wrapper around Stream that contains all the CMAP information needed to maintain a connection.
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Connection {
    /// Driver-generated ID for the connection.
    pub(crate) id: u32,

    /// Server-generated ID for the connection.
    pub(crate) server_id: Option<i64>,

    pub(crate) address: ServerAddress,

    pub(crate) generation: ConnectionGeneration,

    pub(crate) time_created: Instant,

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

    /// Stores a network error encountered while reading or writing. Once the connection has
    /// received an error, it should not be used again and will be closed upon check-in to the
    /// pool.
    error: Option<Error>,

    /// Whether the most recently received message included the moreToCome flag, indicating the
    /// server may send more responses without any additional requests. Attempting to send new
    /// messages on this connection while this value is true will return an error. This value
    /// will remain true until a server response does not include the moreToComeFlag.
    more_to_come: bool,

    stream: BufStream<AsyncStream>,

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

    /// Type responsible for emitting events related to this connection. This is None for
    /// monitoring connections as we do not emit events for those.
    #[derivative(Debug = "ignore")]
    event_emitter: Option<CmapEventEmitter>,

    /// The token callback for OIDC authentication.
    #[derivative(Debug = "ignore")]
    pub(crate) oidc_access_token: Option<String>,
}

impl Connection {
    fn new(
        address: ServerAddress,
        stream: AsyncStream,
        id: u32,
        generation: ConnectionGeneration,
        time_created: Instant,
    ) -> Self {
        Self {
            id,
            server_id: None,
            generation,
            time_created,
            pool_manager: None,
            command_executing: false,
            ready_and_available_time: None,
            stream: BufStream::new(stream),
            address,
            event_emitter: None,
            stream_description: None,
            error: None,
            pinned_sender: None,
            compressor: None,
            more_to_come: false,
            oidc_access_token: None,
        }
    }

    /// Create a connection intended to be stored in a connection pool for operation execution.
    /// TODO: RUST-1454 Remove this from `Connection`, instead wrap a `Connection` type in a
    /// separate type specific to pool.
    pub(crate) fn new_pooled(pending_connection: PendingConnection, stream: AsyncStream) -> Self {
        let generation = match pending_connection.generation {
            PoolGeneration::Normal(gen) => ConnectionGeneration::Normal(gen),
            PoolGeneration::LoadBalanced(_) => ConnectionGeneration::LoadBalanced(None),
        };
        let mut conn = Self::new(
            pending_connection.address,
            stream,
            pending_connection.id,
            generation,
            pending_connection.time_created,
        );
        conn.event_emitter = Some(pending_connection.event_emitter);
        conn
    }

    /// Create a connection intended for monitoring purposes.
    /// TODO: RUST-1454 Rename this to just `new`, drop the pooling-specific data.
    pub(crate) fn new_monitoring(address: ServerAddress, stream: AsyncStream, id: u32) -> Self {
        Self::new(
            address,
            stream,
            id,
            ConnectionGeneration::Monitoring,
            Instant::now(),
        )
    }

    pub(crate) fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            id: self.id,
            server_id: self.server_id,
            address: self.address.clone(),
        }
    }

    pub(crate) fn service_id(&self) -> Option<ObjectId> {
        self.stream_description
            .as_ref()
            .and_then(|sd| sd.service_id)
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
        self.error.is_some()
    }

    /// Helper to create a `ConnectionCheckedOutEvent` for the connection.
    pub(super) fn checked_out_event(&self, time_started: Instant) -> ConnectionCheckedOutEvent {
        ConnectionCheckedOutEvent {
            address: self.address.clone(),
            connection_id: self.id,
            duration: Instant::now() - time_started,
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
            duration: Instant::now() - self.time_created,
        }
    }

    /// Helper to create a `ConnectionClosedEvent` for the connection.
    pub(super) fn closed_event(&self, reason: ConnectionClosedReason) -> ConnectionClosedEvent {
        ConnectionClosedEvent {
            address: self.address.clone(),
            connection_id: self.id,
            reason,
            #[cfg(feature = "tracing-unstable")]
            error: self.error.clone(),
        }
    }

    pub(crate) async fn send_message(
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

        if let Err(ref err) = write_result {
            self.error = Some(err.clone());
        }
        write_result?;

        let response_message_result = Message::read_from(
            &mut self.stream,
            self.stream_description
                .as_ref()
                .map(|d| d.max_message_size_bytes),
        )
        .await;
        self.command_executing = false;
        if let Err(ref err) = response_message_result {
            self.error = Some(err.clone());
        }

        let response_message = response_message_result?;
        self.more_to_come = response_message.flags.contains(MessageFlags::MORE_TO_COME);

        Ok(RawCommandResponse::new(
            self.address.clone(),
            response_message,
        ))
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
        let message = Message::from_command(command, request_id.into())?;
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
        if let Err(ref err) = response_message_result {
            self.error = Some(err.clone());
        }

        let response_message = response_message_result?;
        self.more_to_come = response_message.flags.contains(MessageFlags::MORE_TO_COME);

        Ok(RawCommandResponse::new(
            self.address.clone(),
            response_message,
        ))
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
        if let Some(ref event_emitter) = self.event_emitter {
            event_emitter.emit_event(|| self.closed_event(reason).into());
        }
    }

    /// Nullify the inner state and return it in a new `Connection` for checking back in to
    /// the pool.
    fn take(&mut self) -> Connection {
        Connection {
            id: self.id,
            server_id: self.server_id,
            address: self.address.clone(),
            generation: self.generation,
            time_created: self.time_created,
            stream: std::mem::replace(&mut self.stream, BufStream::new(AsyncStream::Null)),
            event_emitter: self.event_emitter.take(),
            stream_description: self.stream_description.take(),
            command_executing: self.command_executing,
            error: self.error.take(),
            pool_manager: None,
            ready_and_available_time: None,
            pinned_sender: self.pinned_sender.clone(),
            compressor: self.compressor.clone(),
            more_to_come: false,
            oidc_access_token: self.oidc_access_token.take(),
        }
    }

    /// Whether or not the previous command response indicated that the server may send
    /// more responses without another request.
    pub(crate) fn is_streaming(&self) -> bool {
        self.more_to_come
    }

    /// Whether the connection supports sessions.
    pub(crate) fn supports_sessions(&self) -> bool {
        self.stream_description
            .as_ref()
            .and_then(|sd| sd.logical_session_timeout)
            .is_some()
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

#[derive(Debug, Clone, Copy)]
pub(crate) struct LoadBalancedGeneration {
    pub(crate) generation: u32,
    pub(crate) service_id: ObjectId,
}

/// TODO: RUST-1454 Once we have separate types for pooled and non-pooled connections, the
/// monitoring case and the Option<> wrapper can be dropped from this.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ConnectionGeneration {
    Monitoring,
    Normal(u32),
    LoadBalanced(Option<LoadBalancedGeneration>),
}

impl ConnectionGeneration {
    pub(crate) fn service_id(self) -> Option<ObjectId> {
        match self {
            ConnectionGeneration::LoadBalanced(Some(gen)) => Some(gen.service_id),
            _ => None,
        }
    }

    pub(crate) fn is_stale(self, current_generation: &PoolGeneration) -> bool {
        match (self, current_generation) {
            (ConnectionGeneration::Normal(cgen), PoolGeneration::Normal(pgen)) => cgen != *pgen,
            (ConnectionGeneration::LoadBalanced(cgen), PoolGeneration::LoadBalanced(gen_map)) => {
                if let Some(cgen) = cgen {
                    cgen.generation != *gen_map.get(&cgen.service_id).unwrap_or(&0)
                } else {
                    // In the event that an error occurred during handshake and no serviceId was
                    // returned, just ignore the error for SDAM purposes, since
                    // we won't know which serviceId to clear for.
                    false
                }
            }
            _ => load_balanced_mode_mismatch!(false),
        }
    }
}

impl From<LoadBalancedGeneration> for ConnectionGeneration {
    fn from(gen: LoadBalancedGeneration) -> Self {
        ConnectionGeneration::LoadBalanced(Some(gen))
    }
}

/// Struct encapsulating the information needed to establish a `Connection`.
///
/// Creating a `PendingConnection` contributes towards the total connection count of a pool, despite
/// not actually making a TCP connection to the pool's endpoint. This models a "pending" Connection
/// from the CMAP specification.
pub(crate) struct PendingConnection {
    pub(crate) id: u32,
    pub(crate) address: ServerAddress,
    pub(crate) generation: PoolGeneration,
    pub(crate) event_emitter: CmapEventEmitter,
    pub(crate) time_created: Instant,
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
