use tokio::sync::{mpsc, oneshot};

use super::{worker::PoolWorkerHandle, Connection};
use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    runtime::AsyncJoinHandle,
    RUNTIME,
};
use std::time::Duration;

/// Handle for requesting Connections from the pool.
#[derive(Clone, Debug)]
pub(super) struct ConnectionRequester {
    address: StreamAddress,
    sender: mpsc::UnboundedSender<oneshot::Sender<RequestedConnection>>,
    handle: PoolWorkerHandle,
}

impl ConnectionRequester {
    /// Returns a new requester/receiver pair.
    pub(super) fn new(
        address: StreamAddress,
        handle: PoolWorkerHandle,
    ) -> (Self, ConnectionRequestReceiver) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                address,
                sender,
                handle,
            },
            ConnectionRequestReceiver { receiver },
        )
    }

    /// Request a connection from the pool that owns the receiver end of this requester.
    /// Returns an error if it takes longer than wait_queue_timeout before either a connection is
    /// received or an establishment begins.
    pub(super) async fn request(&self, wait_queue_timeout: Option<Duration>) -> Result<Connection> {
        let (sender, receiver) = oneshot::channel();

        // this only errors if the receiver end is dropped, which can't happen because
        // we own a handle to the worker, keeping it alive.
        self.sender.send(sender).unwrap();

        // similarly, the receiver only returns an error if the sender is dropped, which
        // can't happen due to the handle.
        let response = match wait_queue_timeout {
            Some(timeout) => RUNTIME
                .timeout(timeout, receiver)
                .await
                .map(|r| r.unwrap())
                .map_err(|_| {
                    ErrorKind::WaitQueueTimeoutError {
                        address: self.address.clone(),
                    }
                    .into()
                }),
            None => Ok(receiver.await.unwrap()),
        };

        match response {
            Ok(RequestedConnection::Pooled(c)) => Ok(c),
            Ok(RequestedConnection::Establishing(task)) => task.await,
            Err(e) => Err(e),
        }
    }
}

/// Receiving end of a given ConnectionRequester.
#[derive(Debug)]
pub(super) struct ConnectionRequestReceiver {
    receiver: mpsc::UnboundedReceiver<oneshot::Sender<RequestedConnection>>,
}

impl ConnectionRequestReceiver {
    pub(super) async fn recv(&mut self) -> Option<ConnectionRequest> {
        self.receiver
            .recv()
            .await
            .map(|sender| ConnectionRequest { sender })
    }
}

#[derive(Debug)]
pub(super) struct ConnectionRequest {
    sender: oneshot::Sender<RequestedConnection>,
}

impl ConnectionRequest {
    pub(super) fn fulfill(
        self,
        conn: RequestedConnection,
    ) -> std::result::Result<(), RequestedConnection> {
        self.sender.send(conn)
    }
}

#[derive(Debug)]
pub(super) enum RequestedConnection {
    Pooled(Connection),
    Establishing(AsyncJoinHandle<Result<Connection>>),
}

impl RequestedConnection {
    pub(super) fn unwrap_pooled_connection(self) -> Connection {
        match self {
            RequestedConnection::Pooled(c) => c,
            _ => panic!("attempted to unwrap pooled connection when was establishing"),
        }
    }
}
