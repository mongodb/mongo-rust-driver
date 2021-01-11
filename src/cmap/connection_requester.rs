use tokio::sync::{mpsc, oneshot};

use super::{worker::PoolWorkerHandle, Connection};
use crate::{error::Result, options::StreamAddress, runtime::AsyncJoinHandle, RUNTIME};
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
        ConnectionRequestReceiver { receiver },
    )
}

/// Handle for requesting Connections from the pool.
/// This requester will keep the pool alive. Once all requesters have been dropped,
/// the pool will stop servicing requests, drop its available connections, and close.
#[derive(Clone, Debug)]
pub(super) struct ConnectionRequester {
    address: StreamAddress,
    sender: mpsc::UnboundedSender<oneshot::Sender<ConnectionRequestResult>>,
    handle: PoolWorkerHandle,
}

impl ConnectionRequester {
    /// Request a connection from the pool that owns the receiver end of this requester.
    /// Returns None if it takes longer than wait_queue_timeout before the pool returns a result.
    pub(super) async fn request(
        &self,
        wait_queue_timeout: Option<Duration>,
    ) -> Option<ConnectionRequestResult> {
        let (sender, receiver) = oneshot::channel();

        // this only errors if the receiver end is dropped, which can't happen because
        // we own a handle to the worker, keeping it alive.
        self.sender.send(sender).unwrap();

        match wait_queue_timeout {
            Some(timeout) => RUNTIME
                .timeout(timeout, receiver)
                .await
                .map(|r| r.unwrap()) // see comment below as to why this is safe
                .ok(),

            // similarly, the receiver only returns an error if the sender is dropped, which
            // can't happen due to the handle.
            None => Some(receiver.await.unwrap()),
        }
    }
}

/// Receiving end of a given ConnectionRequester.
#[derive(Debug)]
pub(super) struct ConnectionRequestReceiver {
    receiver: mpsc::UnboundedReceiver<oneshot::Sender<ConnectionRequestResult>>,
}

impl ConnectionRequestReceiver {
    pub(super) async fn recv(&mut self) -> Option<ConnectionRequest> {
        self.receiver
            .recv()
            .await
            .map(|sender| ConnectionRequest { sender })
    }
}

/// Struct encapsulating a request for a connection.
#[derive(Debug)]
pub(super) struct ConnectionRequest {
    sender: oneshot::Sender<ConnectionRequestResult>,
}

impl ConnectionRequest {
    /// Respond to the connection request, either with a pooled connection or one that is
    /// establishing asynchronously.
    pub(super) fn fulfill(
        self,
        result: ConnectionRequestResult,
    ) -> std::result::Result<(), ConnectionRequestResult> {
        self.sender.send(result)
    }
}

#[derive(Debug)]
pub(super) enum ConnectionRequestResult {
    /// A connection that was already established and was simply checked out of the pool.
    Pooled(Connection),

    /// A new connection in the process of being established.
    /// The handle can be awaited upon to receive the established connection.
    Establishing(AsyncJoinHandle<Result<Connection>>),

    /// The request was rejected because the pool was cleared before it could
    /// be fulfilled.
    PoolCleared,
}

impl ConnectionRequestResult {
    pub(super) fn unwrap_pooled_connection(self) -> Connection {
        match self {
            ConnectionRequestResult::Pooled(c) => c,
            _ => panic!("attempted to unwrap pooled connection when was establishing"),
        }
    }
}
