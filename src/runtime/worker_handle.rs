use tokio::sync::mpsc;

/// Handle to the worker. Once all handles have been dropped, the worker
/// will stop waiting for new requests and drop the pool itself.
#[derive(Debug, Clone)]
pub(crate) struct WorkerHandle {
    _sender: mpsc::Sender<()>,
}

impl PoolWorkerHandle {
    #[cfg(test)]
    pub(super) fn new_mocked() -> Self {
        let (s, _) = handle_channel();
        s
    }
}

/// Listener used to determine when all handles have been dropped.
#[derive(Debug)]
struct HandleListener {
    receiver: mpsc::Receiver<()>,
}

impl HandleListener {
    /// Listen until all handles are dropped.
    /// This will not return until all handles are dropped, so make sure to only poll this via
    /// select or with a timeout.
    async fn wait_for_all_handle_drops(&mut self) {
        self.receiver.recv().await;
    }
}
