use tokio::sync::mpsc;

/// Handle to a worker. Once all handles have been dropped, the worker
/// will stop waiting for new requests.
#[derive(Debug, Clone)]
pub(crate) struct WorkerHandle {
    _sender: mpsc::Sender<()>,
}

impl WorkerHandle {
    #[cfg(test)]
    pub(crate) fn new_mocked() -> Self {
        let (s, _) = WorkerHandleListener::channel();
        s
    }
}

/// Listener used to determine when all handles have been dropped.
#[derive(Debug)]
pub(crate) struct WorkerHandleListener {
    receiver: mpsc::Receiver<()>,
}

impl WorkerHandleListener {
    /// Listen until all handles are dropped.
    /// This will not return until all handles are dropped, so make sure to only poll this via
    /// select or with a timeout.
    pub(crate) async fn wait_for_all_handle_drops(&mut self) {
        self.receiver.recv().await;
    }

    /// Returns whether there are handles that have not been dropped yet.
    pub(crate) fn check_if_alive(&mut self) -> bool {
        match self.receiver.try_recv() {
            Err(mpsc::error::TryRecvError::Disconnected) => false,
            _ => true,
        }
    }

    /// Constructs a new channel for for monitoring whether this worker still has references
    /// to it.
    pub(crate) fn channel() -> (WorkerHandle, WorkerHandleListener) {
        let (sender, receiver) = mpsc::channel(1);
        (
            WorkerHandle { _sender: sender },
            WorkerHandleListener { receiver },
        )
    }
}
