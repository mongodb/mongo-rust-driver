use std::sync::atomic::Ordering;

use futures_util::future::join_all;

use crate::action::action_impl;

#[action_impl]
impl Action for crate::action::Shutdown {
    type Future = ShutdownFuture;

    async fn execute(self) -> () {
        if !self.immediate {
            // Subtle bug: if this is inlined into the `join_all(..)` call, Rust will extend the
            // lifetime of the temporary unnamed `MutexLock` until the end of the *statement*,
            // causing the lock to be held for the duration of the join, which deadlocks.
            let pending = self
                .client
                .inner
                .shutdown
                .pending_drops
                .lock()
                .unwrap()
                .extract();
            join_all(pending).await;
        }
        self.client.inner.topology.shutdown().await;
        // This has to happen last to allow pending cleanup to execute commands.
        self.client
            .inner
            .shutdown
            .executed
            .store(true, Ordering::SeqCst);
    }
}
