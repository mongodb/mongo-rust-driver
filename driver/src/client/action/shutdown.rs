use std::sync::atomic::Ordering;

use crate::action::action_impl;

#[action_impl]
impl Action for crate::action::Shutdown {
    type Future = ShutdownFuture;

    async fn execute(self) -> () {
        self.client.inner.shutdown.close();
        if !self.immediate {
            self.client.inner.shutdown.wait().await;
        }
        // If shutdown has already been called on a different copy of the client, don't call
        // end_all_sessions again.
        if !self.client.inner.shutdown_done.load(Ordering::SeqCst) {
            self.client.end_all_sessions().await;
        }
        self.client.inner.topology.updater().shutdown().await;
        // This has to happen last to allow pending cleanup to execute commands.
        self.client
            .inner
            .shutdown_done
            .store(true, Ordering::SeqCst);
    }
}
