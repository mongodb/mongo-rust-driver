use std::time::Duration;

use tokio::sync::broadcast::{self, Receiver, Sender};

use crate::RUNTIME;

/// Provides functionality for message passing between server selection operations and SDAM
/// background tasks.
#[derive(Clone, Debug)]
pub(crate) struct TopologyMessageManager {
    topology_check_requester: Sender<()>,
    topology_change_notifier: Sender<()>,
}

impl TopologyMessageManager {
    /// Constructs a new TopologyMessageManager.
    pub(super) fn new() -> Self {
        let (topology_check_requester, _) = broadcast::channel(1);
        let (topology_change_notifier, _) = broadcast::channel(1);

        Self {
            topology_check_requester,
            topology_change_notifier,
        }
    }

    /// Requests that the SDAM background tasks check the topology immediately. This should be
    /// called by each server selection operation when it fails to select a server.
    pub(super) fn request_topology_check(&self) {
        let _: Result<_, _> = self.topology_check_requester.send(());
    }

    /// Notifies the server selection operations that the topology has changed. This should be
    /// called by SDAM background tasks after a topology check if the topology has changed.
    pub(super) fn notify_topology_changed(&self) {
        let _: Result<_, _> = self.topology_change_notifier.send(());
    }

    pub(super) fn subscribe_to_topology_check_requests(&self) -> TopologyMessageSubscriber {
        TopologyMessageSubscriber::new(self.topology_check_requester.subscribe())
    }

    pub(super) fn subscribe_to_topology_changes(&self) -> TopologyMessageSubscriber {
        TopologyMessageSubscriber::new(self.topology_change_notifier.subscribe())
    }
}

pub(crate) struct TopologyMessageSubscriber {
    receiver: Receiver<()>,
}

impl TopologyMessageSubscriber {
    fn new(receiver: Receiver<()>) -> Self {
        Self { receiver }
    }

    /// Waits for either `timeout` to elapse or a message to be received.
    /// Returns true if a message was received, false for a timeout.
    pub(crate) async fn wait_for_message(&mut self, timeout: Duration) -> bool {
        RUNTIME.timeout(timeout, self.receiver.recv()).await.is_ok()
    }
}
