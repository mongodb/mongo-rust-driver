use std::{future::IntoFuture, sync::atomic::Ordering};

use futures_util::{future::join_all, FutureExt};

use crate::BoxFuture;

impl IntoFuture for crate::action::Shutdown {
    type Output = ();

    type IntoFuture = BoxFuture<'static, ()>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
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
        .boxed()
    }
}

/// Opaque future type for action execution.
pub struct ShutdownFuture(BoxFuture<'static, ()>);

impl std::future::Future for ShutdownFuture {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}