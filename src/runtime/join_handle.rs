use std::{
    future::Future,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

/// A handle used for awaiting on tasks spawned in `AsyncRuntime::execute`.
///
/// Note: the `Future::Output` of this handle is `Result<T>`, not just `T`.
#[derive(Debug)]
pub(crate) enum AsyncJoinHandle<T> {
    /// Wrapper around `tokio::task:JoinHandle`.
    Tokio(tokio::task::JoinHandle<T>),
}

impl<T> Future for AsyncJoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.deref_mut() {
            // Tokio wraps the task's return value with a `Result` that catches panics; in our case
            // we want to propagate the panic, so for once `unwrap` is the right tool to use.
            Self::Tokio(ref mut handle) => Pin::new(handle).poll(cx).map(|result| result.unwrap()),
        }
    }
}
