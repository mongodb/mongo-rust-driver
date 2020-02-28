use std::{sync::Arc, time::Duration};

use futures::future::Either;
use futures_timer::Delay;
use tokio::sync::watch::{self, Receiver, Sender};

/// Provides functionality for message passing between server selection operations and SDAM
/// background tasks.
#[derive(Clone, Debug)]
pub(crate) struct TopologyMessageManager {
    topology_check_requester: Arc<Sender<()>>,

    topology_check_listener: Receiver<()>,

    topology_change_notifier: Arc<Sender<()>>,

    topology_change_listener: Receiver<()>,
}

impl TopologyMessageManager {
    /// Constructs a new TopologyMessageManager.
    pub(super) fn new() -> Self {
        let (topology_check_requester, topology_check_listener) = watch::channel(());
        let (topology_change_notifier, topology_change_listener) = watch::channel(());

        Self {
            topology_check_requester: Arc::new(topology_check_requester),
            topology_check_listener,
            topology_change_notifier: Arc::new(topology_change_notifier),
            topology_change_listener,
        }
    }

    /// Gets a receiver which will receive messages whenever a topology check has been requested by
    /// a server selection operation. This should be called by each SDAM background task when it
    /// starts up.
    pub(super) fn subscribe_to_topology_check_requests(&self) -> Receiver<()> {
        self.topology_check_listener.clone()
    }

    /// Requests that the SDAM background tasks check the topology immediately. This should be
    /// called by each server selection operation when it fails to select a server.
    pub(super) fn request_topology_check(&self) {
        let _ = self.topology_check_requester.broadcast(());
    }

    /// Notifies the server selection operations that the topology has changed. This should be
    /// called by SDAM background tasks after a topology check if the topology has changed.
    pub(super) fn notify_topology_changed(&self) {
        let _ = self.topology_change_notifier.broadcast(());
    }

    /// Waits for either `timeout` to elapse or a topology check to be requested by SDAM background
    /// tasks.
    ///
    /// Returns `true` if a topology check has been requested or `false` if the timeout elapsed.
    pub(super) async fn wait_for_topology_check_request(&self, timeout: Duration) -> bool {
        let mut listener = self.topology_check_listener.clone();

        wait_for_notification(&mut listener, timeout).await
    }

    /// Waits for either `timeout` to elapse or the topology to change.
    ///
    /// Returns `true` if the topology has changed or `false` if the timeout elapsed.
    pub(crate) async fn wait_for_topology_change(&self, timeout: Duration) -> bool {
        let mut listener = self.topology_change_listener.clone();

        // Per the tokio docs, the first call to `tokio::watch::Receiver::recv` will return
        // immediately with the current value, so we skip over this and wait for the next message.
        let _ = listener.recv().await;

        wait_for_notification(&mut listener, timeout).await
    }
}

async fn wait_for_notification(receiver: &mut Receiver<()>, timeout: Duration) -> bool {
    let timeout = Delay::new(timeout);
    let message_received = Box::pin(receiver.recv());

    match futures::future::select(timeout, message_received).await {
        Either::Left(..) => false,
        Either::Right(..) => true,
    }
}
