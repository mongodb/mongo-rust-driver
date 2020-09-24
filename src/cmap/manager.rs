use tokio::sync::mpsc;

use super::Connection;
use crate::error::Error;

#[derive(Debug)]
pub(super) enum PoolManagementRequest {
    Clear,
    Populate(Connection),
    CheckIn(Connection),
    HandleConnectionFailed(Error),
}

impl PoolManagementRequest {
    fn unwrap_check_in(self) -> Connection {
        match self {
            PoolManagementRequest::CheckIn(conn) => conn,
            _ => panic!("tried to unwrap checkin but got {:?}", self),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct PoolManager {
    pub(super) sender: mpsc::UnboundedSender<PoolManagementRequest>,
}

impl PoolManager {
    pub(super) fn new() -> (PoolManager, ManagementRequestReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (Self { sender }, ManagementRequestReceiver { receiver })
    }

    pub(super) fn clear(&self) {
        let _ = self.sender.send(PoolManagementRequest::Clear);
    }

    pub(crate) fn check_in(&self, connection: Connection) -> std::result::Result<(), Connection> {
        if let Err(request) = self.sender.send(PoolManagementRequest::CheckIn(connection)) {
            let conn = request.0.unwrap_check_in();
            return Err(conn);
        }
        Ok(())
    }

    pub(super) fn handle_connection_failed(&self, error: Error) {
        let _ = self
            .sender
            .send(PoolManagementRequest::HandleConnectionFailed(error));
    }

    pub(super) fn populate_connection(&self, connection: Connection) {
        let _ = self
            .sender
            .send(PoolManagementRequest::Populate(connection));
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
