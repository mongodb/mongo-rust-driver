use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::{Duration, Instant},
};

use derive_where::derive_where;
use tokio::sync::{mpsc, Mutex};

use super::{
    CmapEventEmitter,
    Connection,
    ConnectionGeneration,
    ConnectionInfo,
    PendingConnection,
    PinnedConnectionHandle,
    PoolManager,
};
use crate::{
    bson::oid::ObjectId,
    cmap::PoolGeneration,
    error::{Error, Result},
    event::cmap::{
        ConnectionCheckedInEvent,
        ConnectionCheckedOutEvent,
        ConnectionClosedEvent,
        ConnectionClosedReason,
        ConnectionReadyEvent,
    },
    runtime::AsyncStream,
};

/// A wrapper around the [`Connection`] type that represents a connection within a connection pool.
/// This type derefs into [`Connection`], so fields and methods exposed on that type can be called
/// directly from this one.
#[derive_where(Debug)]
pub(crate) struct PooledConnection {
    connection: Connection,

    /// The connection pool generation from which this connection was checked out.
    pub(crate) generation: ConnectionGeneration,

    /// Marks the time when the connection was last checked into the pool. This value can be used
    /// to determine whether this connection is idle.
    ready_and_available_time: Option<Instant>,

    /// The manager used to check this connection back into the pool when dropped. This value is
    /// unset when the connection is checked into the pool.
    pool_manager: Option<PoolManager>,

    /// A sender to return this connection to its pinner.
    pinned_sender: Option<mpsc::Sender<PooledConnection>>,

    /// Emitter for events related to this connection.
    #[derive_where(skip)]
    event_emitter: CmapEventEmitter,
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl PooledConnection {
    pub(crate) fn new(pending_connection: PendingConnection, stream: AsyncStream) -> Self {
        let connection = Connection::new(
            pending_connection.address,
            stream,
            pending_connection.id,
            pending_connection.time_created,
        );
        let generation = match pending_connection.generation {
            PoolGeneration::Normal(generation) => ConnectionGeneration::Normal(generation),
            PoolGeneration::LoadBalanced(_) => ConnectionGeneration::LoadBalanced(None),
        };
        Self {
            connection,
            generation,
            ready_and_available_time: None,
            pool_manager: None,
            pinned_sender: None,
            event_emitter: pending_connection.event_emitter,
        }
    }

    pub(crate) fn info(&self) -> ConnectionInfo {
        ConnectionInfo {
            id: self.connection.id,
            server_id: self.server_id,
            address: self.connection.address.clone(),
        }
    }

    /// The service ID for this connection. Only returns a value if this connection is to a load
    /// balancer.
    pub(crate) fn service_id(&self) -> Option<ObjectId> {
        self.stream_description
            .as_ref()
            .and_then(|sd| sd.service_id)
    }

    /// Updates the state of the connection to indicate that it is checked into the pool.
    pub(crate) fn mark_as_available(&mut self) {
        self.pool_manager.take();
        self.ready_and_available_time = Some(Instant::now());
    }

    /// Updates the state of the connection to indicate that it is checked out of the pool.
    pub(crate) fn mark_as_in_use(&mut self, manager: PoolManager) {
        self.pool_manager = Some(manager);
        self.ready_and_available_time.take();
    }

    /// Whether this connection is idle.
    pub(crate) fn is_idle(&self, max_idle_time: Option<Duration>) -> bool {
        self.ready_and_available_time
            .and_then(|ready_and_available_time| {
                max_idle_time.map(|max_idle_time| {
                    Instant::now().duration_since(ready_and_available_time) >= max_idle_time
                })
            })
            .unwrap_or(false)
    }

    /// Nullify the inner state and return it in a new [`Connection`] for checking back in to
    /// the pool.
    fn take(&mut self) -> Self {
        Self {
            connection: self.connection.take(),
            generation: self.generation,
            event_emitter: self.event_emitter.clone(),
            pool_manager: None,
            ready_and_available_time: None,
            pinned_sender: self.pinned_sender.clone(),
        }
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

    /// Close this connection, emitting a [`ConnectionClosedEvent`] with the supplied reason.
    pub(crate) fn close_and_drop(mut self, reason: ConnectionClosedReason) {
        self.close(reason);
    }

    /// Close this connection, emitting a [`ConnectionClosedEvent`] with the supplied reason.
    fn close(&mut self, reason: ConnectionClosedReason) {
        self.pool_manager.take();
        self.event_emitter
            .emit_event(|| self.closed_event(reason).into());
    }

    /// Whether the connection supports sessions.
    pub(crate) fn supports_sessions(&self) -> bool {
        self.connection
            .stream_description
            .as_ref()
            .and_then(|sd| sd.logical_session_timeout)
            .is_some()
    }

    /// Helper to create a [`ConnectionCheckedOutEvent`] for the connection.
    pub(crate) fn checked_out_event(&self, time_started: Instant) -> ConnectionCheckedOutEvent {
        ConnectionCheckedOutEvent {
            address: self.connection.address.clone(),
            connection_id: self.connection.id,
            duration: Instant::now() - time_started,
        }
    }

    /// Helper to create a [`ConnectionCheckedInEvent`] for the connection.
    pub(crate) fn checked_in_event(&self) -> ConnectionCheckedInEvent {
        ConnectionCheckedInEvent {
            address: self.connection.address.clone(),
            connection_id: self.connection.id,
        }
    }

    /// Helper to create a [`ConnectionReadyEvent`] for the connection.
    pub(crate) fn ready_event(&self) -> ConnectionReadyEvent {
        ConnectionReadyEvent {
            address: self.connection.address.clone(),
            connection_id: self.connection.id,
            duration: Instant::now() - self.connection.time_created,
        }
    }

    /// Helper to create a [`ConnectionClosedEvent`] for the connection.
    pub(super) fn closed_event(&self, reason: ConnectionClosedReason) -> ConnectionClosedEvent {
        ConnectionClosedEvent {
            address: self.connection.address.clone(),
            connection_id: self.connection.id,
            reason,
            #[cfg(feature = "tracing-unstable")]
            error: self.connection.error.clone(),
        }
    }
}

impl Drop for PooledConnection {
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
