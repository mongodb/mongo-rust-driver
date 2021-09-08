use tokio::sync::{mpsc, oneshot};

use super::Connection;
use crate::{
    bson::oid::ObjectId,
    error::{Error, Result},
    runtime::AcknowledgedMessage,
};

pub(super) fn channel() -> (PoolManager, ManagementRequestReceiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        PoolManager { sender },
        ManagementRequestReceiver { receiver },
    )
}

/// Struct used to make management requests to the pool (e.g. checking in a connection).
/// A PoolManager will NOT keep a pool from going out of scope and closing.
#[derive(Clone, Debug)]
pub(super) struct PoolManager {
    sender: mpsc::UnboundedSender<PoolManagementRequest>,
}

impl PoolManager {
    /// Lazily clear the pool.
    pub(super) async fn clear(&self, cause: Error, service_id: Option<ObjectId>) {
        let (message, acknowledgment_receiver) = AcknowledgedMessage::package(());
        if self
            .sender
            .send(PoolManagementRequest::Clear {
                completion_handler: message,
                cause,
                service_id,
            })
            .is_ok()
        {
            acknowledgment_receiver.wait_for_acknowledgment().await;
        }
    }

    /// Mark the pool as "ready" as per the CMAP specification.
    pub(super) async fn mark_as_ready(&self) {
        let (message, acknowledgment_receiver) = AcknowledgedMessage::package(());
        if self
            .sender
            .send(PoolManagementRequest::MarkAsReady {
                completion_handler: message,
            })
            .is_ok()
        {
            acknowledgment_receiver.wait_for_acknowledgment().await;
        }
    }

    /// Check in the given connection to the pool.
    /// This returns an error containing the connection if the pool has been dropped already.
    pub(crate) fn check_in(&self, connection: Connection) -> std::result::Result<(), Connection> {
        if let Err(request) = self.sender.send(PoolManagementRequest::CheckIn(connection)) {
            let conn = request.0.unwrap_check_in();
            return Err(conn);
        }
        Ok(())
    }

    /// Store a pinned connection for later retrieval via id.  This connection is removed from the normal connection pool until unpinned.
    pub(super) fn store_pinned(
        &self,
        connection: Connection,
    ) -> std::result::Result<(), Connection> {
        if let Err(request) = self
            .sender
            .send(PoolManagementRequest::StorePinned(connection))
        {
            let conn = request.0.unwrap_check_in();
            return Err(conn);
        }
        Ok(())
    }

    /// Retrieve a previously-stored pinned connection by id.  This returns an error if the connection is already in use.
    pub(super) async fn take_pinned(&self, id: u32) -> Result<Connection> {
        let (tx, rx) = oneshot::channel();
        if self
            .sender
            .send(PoolManagementRequest::TakePinned {
                connection_id: id,
                sender: tx,
            })
            .is_err()
        {
            return Err(Error::internal("pool worker dropped"));
        }
        match rx.await {
            Ok(Some(conn)) => Ok(conn),
            Ok(None) => Err(Error::internal(format!(
                "no pinned connection with id = {}",
                id
            ))),
            Err(_) => Err(Error::internal("pool sender dropped")),
        }
    }

    /// Remove a connection from pinned storage.
    pub(super) fn unpin(&self, id: u32) {
        let _ = self.sender.send(PoolManagementRequest::Unpin(id));
    }

    /// Notify the pool that establishing a connection failed.
    pub(super) fn handle_connection_failed(&self) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionFailed);
    }

    /// Notify the pool that establishing a connection succeeded.
    pub(super) fn handle_connection_succeeded(&self, conn: ConnectionSucceeded) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionSucceeded(conn));
    }
}

#[derive(Debug)]
pub(super) struct ManagementRequestReceiver {
    pub(super) receiver: mpsc::UnboundedReceiver<PoolManagementRequest>,
}

impl ManagementRequestReceiver {
    pub(super) async fn recv(&mut self) -> Option<PoolManagementRequest> {
        self.receiver.recv().await
    }
}

#[derive(Debug)]
pub(super) enum PoolManagementRequest {
    /// Clear the pool, transitioning it to Paused.
    Clear {
        completion_handler: AcknowledgedMessage<()>,
        cause: Error,
        service_id: Option<ObjectId>,
    },

    /// Mark the pool as Ready, allowing connections to be created and checked out.
    MarkAsReady {
        completion_handler: AcknowledgedMessage<()>,
    },

    /// Check in the given connection.
    CheckIn(Connection),

    /// Store the given pinned connection.
    StorePinned(Connection),

    /// Take a previously-stored pinned connection.
    TakePinned {
        connection_id: u32,
        sender: oneshot::Sender<Option<Connection>>,
    },

    /// Unpin a pinned connection and return it to the pool.
    Unpin(u32),

    /// Update the pool based on the given establishment error.
    HandleConnectionFailed,

    /// Update the pool after a successful connection, optionally populating the pool
    /// with the successful connection.
    HandleConnectionSucceeded(ConnectionSucceeded),
}

impl PoolManagementRequest {
    fn unwrap_check_in(self) -> Connection {
        match self {
            PoolManagementRequest::CheckIn(conn) => conn,
            _ => panic!("tried to unwrap checkin but got {:?}", self),
        }
    }
}

#[derive(Debug)]
pub(super) enum ConnectionSucceeded {
    ForPool(Connection),
    Used { service_id: Option<ObjectId> },
}

impl ConnectionSucceeded {
    pub(super) fn service_id(&self) -> Option<ObjectId> {
        match self {
            ConnectionSucceeded::ForPool(conn) => conn.generation.service_id(),
            ConnectionSucceeded::Used { service_id, .. } => *service_id,
        }
    }
}
