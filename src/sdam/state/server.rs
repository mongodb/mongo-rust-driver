use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use crate::{
    cmap::{options::ConnectionPoolOptions, ConnectionPool},
    error::Error,
    options::{ClientOptions, StreamAddress},
    runtime::HttpClient,
    sdam::monitor::Monitor,
};

/// Contains the state for a given server in the topology.
#[derive(Debug)]
pub(crate) struct Server {
    pub(crate) address: StreamAddress,

    /// The connection pool for the server.
    pub(crate) pool: ConnectionPool,

    /// Number of operations currently using this server.
    operation_count: AtomicU32,
}

impl Server {
    #[cfg(test)]
    pub(crate) fn new_mocked(address: StreamAddress, operation_count: u32) -> Self {
        Self {
            address: address.clone(),
            pool: ConnectionPool::new_mocked(address),
            operation_count: AtomicU32::new(operation_count),
        }
    }

    pub(crate) fn create(
        address: StreamAddress,
        options: &ClientOptions,
        http_client: HttpClient,
    ) -> (Arc<Self>, Monitor) {
        let (update_sender, update_receiver) = ServerUpdateSender::channel();
        let server = Arc::new(Self {
            pool: ConnectionPool::new(
                address.clone(),
                http_client,
                update_sender,
                Some(ConnectionPoolOptions::from_client_options(options)),
            ),
            address: address.clone(),
            operation_count: AtomicU32::new(0),
        });
        let monitor = Monitor::new(address, &server, options.clone(), update_receiver);
        (server, monitor)
    }

    pub(crate) fn increment_operation_count(&self) {
        self.operation_count.fetch_add(1, Ordering::SeqCst);
    }

    pub(crate) fn decrement_operation_count(&self) {
        self.operation_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub(crate) fn operation_count(&self) -> u32 {
        self.operation_count.load(Ordering::SeqCst)
    }
}

/// An event that could update the topology's view of a server.
/// TODO: add success cases from application handshakes.
#[derive(Debug)]
pub(crate) enum ServerUpdateReason {
    Error { error: Error, error_generation: u32 },
}

#[derive(Debug)]
pub(crate) struct ServerUpdate {
    pub(crate) reason: ServerUpdateReason,
    acknowledger: tokio::sync::oneshot::Sender<()>,
}

impl ServerUpdate {
    pub(crate) fn acknowledge(self) {
        // other end hanging up is a non-issue
        let _: std::result::Result<_, _> = self.acknowledger.send(());
    }
}

#[derive(Debug)]
pub(crate) struct ServerUpdateReceiver {
    receiver: tokio::sync::mpsc::Receiver<ServerUpdate>,
}

impl ServerUpdateReceiver {
    pub(crate) async fn recv(&mut self) -> Option<ServerUpdate> {
        self.receiver.recv().await
    }
}

/// Struct used to update the topology's view of a given server.
#[derive(Clone, Debug)]
pub(crate) struct ServerUpdateSender {
    sender: tokio::sync::mpsc::Sender<ServerUpdate>,
}

impl ServerUpdateSender {
    /// Create a new sender/receiver pair.
    pub(crate) fn channel() -> (Self, ServerUpdateReceiver) {
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        (
            ServerUpdateSender { sender },
            ServerUpdateReceiver { receiver },
        )
    }

    /// Update the server based on the given error.
    /// This will block until the topology has processed the error.
    pub(crate) async fn handle_error(&mut self, error: Error, error_generation: u32) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let reason = ServerUpdateReason::Error {
            error,
            error_generation,
        };
        let update = ServerUpdate {
            reason,
            acknowledger: sender,
        };

        // These only fails if the other ends hang up, which means the monitor is
        // stopped, so we can just discard this update.
        println!("sending update {:?}", update);
        let _: std::result::Result<_, _> = self.sender.send(update).await;
        let _: std::result::Result<_, _> = receiver.await;
        println!("got ack");
    }
}
