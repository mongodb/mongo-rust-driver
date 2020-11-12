use std::{sync::Arc, time::Duration};

use tokio::sync::watch::{self, Receiver, Sender};

use crate::RUNTIME;

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

    /// Requests that the SDAM background tasks check the topology immediately. This should be
    /// called by each server selection operation when it fails to select a server.
    pub(super) fn request_topology_check(&self) {
        let _ = self.topology_check_requester.send(());
    }

    /// Notifies the server selection operations that the topology has changed. This should be
    /// called by SDAM background tasks after a topology check if the topology has changed.
    pub(super) fn notify_topology_changed(&self) {
        let _ = self.topology_change_notifier.send(());
    }

    pub(super) async fn subscribe_to_topology_check_requests(&self) -> TopologyMessageSubscriber {
        TopologyMessageSubscriber::new(&self.topology_check_listener).await
    }

    pub(super) async fn subscribe_to_topology_changes(&self) -> TopologyMessageSubscriber {
        TopologyMessageSubscriber::new(&self.topology_change_listener).await
    }
}

pub(crate) struct TopologyMessageSubscriber {
    receiver: Receiver<()>,
}

impl TopologyMessageSubscriber {
    async fn new(receiver: &Receiver<()>) -> Self {
        Self {
            receiver: receiver.clone(),
        }
    }

    /// Waits for either `timeout` to elapse or a message to be received.
    /// Returns true if a message was received, false for a timeout.
    pub(crate) async fn wait_for_message(&mut self, timeout: Duration) -> bool {
        RUNTIME
            .timeout(timeout, self.receiver.changed())
            .await
            .is_ok()
    }
}
