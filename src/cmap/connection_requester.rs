use tokio::sync::{mpsc, oneshot};

use super::{worker::PoolWorkerHandle, Connection};
use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
    runtime::AsyncJoinHandle,
    RUNTIME,
};
use std::time::Duration;

/// Returns a new requester/receiver pair.
pub(super) fn channel(
    address: StreamAddress,
    handle: PoolWorkerHandle,
) -> (ConnectionRequester, ConnectionRequestReceiver) {
    let (sender, receiver) = mpsc::unbounded_channel();
    (
        ConnectionRequester {
            address,
            sender,
            handle,
        },
        ConnectionRequestReceiver::new(receiver),
    )
}

/// Handle for requesting Connections from the pool.
/// This requester will keep the pool alive. Once all requesters have been dropped,
/// the pool will stop servicing requests, drop its available connections, and close.
#[derive(Clone, Debug)]
pub(super) struct ConnectionRequester {
    address: StreamAddress,
    sender: mpsc::UnboundedSender<oneshot::Sender<RequestedConnection>>,
    handle: PoolWorkerHandle,
}

impl ConnectionRequester {
    /// Request a connection from the pool that owns the receiver end of this requester.
    /// Returns an error if it takes longer than wait_queue_timeout before either a connection is
    /// received or an establishment begins.
    pub(super) async fn request(&self, wait_queue_timeout: Option<Duration>) -> Result<Connection> {
        let (sender, receiver) = oneshot::channel();

        // this only errors if the receiver end is dropped, which can't happen because
        // we own a handle to the worker, keeping it alive.
        self.sender.send(sender).unwrap();

        let response = match wait_queue_timeout {
            Some(timeout) => RUNTIME
                .timeout(timeout, receiver)
                .await
                .map(|r| r.unwrap()) // see comment below as to why this is safe
                .map_err(|_| {
                    ErrorKind::WaitQueueTimeoutError {
                        address: self.address.clone(),
                    }
                    .into()
                }),

            // similarly, the receiver only returns an error if the sender is dropped, which
            // can't happen due to the handle.
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
    cache: Option<ConnectionRequest>,
}

impl ConnectionRequestReceiver {
    pub(super) fn new(
        receiver: mpsc::UnboundedReceiver<oneshot::Sender<RequestedConnection>>,
    ) -> Self {
        Self {
            receiver,
            cache: None,
        }
    }

    pub(super) async fn recv(&mut self) -> Option<ConnectionRequest> {
        match self.cache.take() {
            Some(request) => Some(request),
            None => self
                .receiver
                .recv()
                .await
                .map(|sender| ConnectionRequest { sender }),
        }
    }

    /// Put a request back into the receiver. Next call to `recv` will immediately
    /// return this value.
    pub(super) fn cache_request(&mut self, request: ConnectionRequest) {
        self.cache = Some(request);
    }
}

/// Struct encapsulating a request for a connection.
#[derive(Debug)]
pub(super) struct ConnectionRequest {
    sender: oneshot::Sender<RequestedConnection>,
}

impl ConnectionRequest {
    /// Respond to the connection request, either with a pooled connection or one that is
    /// establishing asynchronously.
    pub(super) fn fulfill(
        self,
        conn: RequestedConnection,
    ) -> std::result::Result<(), RequestedConnection> {
        self.sender.send(conn)
    }
}

#[derive(Debug)]
pub(super) enum RequestedConnection {
    /// A connection that was already established and was simply checked out of the pool.
    Pooled(Connection),

    /// A new connection in the process of being established.
    /// The handle can be awaited upon to receive the established connection.
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
