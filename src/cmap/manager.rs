use tokio::sync::mpsc;

use super::Connection;
use crate::error::Error;

/// Struct used to make management requests to the pool (e.g. checking in a connection).
/// A PoolManager will NOT keep a pool from going out of scope and closing.
#[derive(Clone, Debug)]
pub(super) struct PoolManager {
    pub(super) sender: mpsc::UnboundedSender<PoolManagementRequest>,
}

impl PoolManager {
    pub(super) fn new() -> (PoolManager, ManagementRequestReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, ManagementRequestReceiver { receiver })
    }

    /// Lazily clear the pool.
    pub(super) fn clear(&self) {
        let _ = self.sender.send(PoolManagementRequest::Clear);
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
    pub(super) fn handle_connection_failed(&self, error: Error) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionFailed(error));
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
    Clear,
    CheckIn(Connection),
    HandleConnectionFailed(Error),
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
