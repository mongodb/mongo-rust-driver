use tokio::sync::mpsc;

use super::Connection;
use crate::runtime::AcknowledgedMessage;

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
    pub(super) async fn clear(&self) {
        let (message, acknowledgment_receiver) = AcknowledgedMessage::package(());
        if self
            .sender
            .send(PoolManagementRequest::Clear(message))
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

    /// Notify the pool that establishing a connection failed.
    pub(super) fn handle_connection_failed(&self) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionFailed);
    }

    /// Notify the pool that establishing a connection succeeded.
    pub(super) fn handle_connection_succeeded(&self, connection: Option<Connection>) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionSucceeded(connection));
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
    Clear(AcknowledgedMessage<()>),

    /// Mark the pool as Ready, allowing connections to be created and checked out.
    MarkAsReady {
        completion_handler: AcknowledgedMessage<()>,
    },

    /// Check in the given connection.
    CheckIn(Connection),

    /// Update the pool based on the given establishment error.
    HandleConnectionFailed,

    /// Update the pool after a successful connection, optionally populating the pool
    /// with the successful connection.
    HandleConnectionSucceeded(Option<Connection>),
}

impl PoolManagementRequest {
    fn unwrap_check_in(self) -> Connection {
        match self {
            PoolManagementRequest::CheckIn(conn) => conn,
            _ => panic!("tried to unwrap checkin but got {:?}", self),
        }
    }
}
