use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

/// A handle used for awaiting on tasks spawned in `AsyncRuntime::execute`.
#[derive(Debug)]
pub(crate) struct AsyncJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> AsyncJoinHandle<T> {
    pub(crate) fn spawn<F>(fut: F) -> Self
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        let handle = tokio::runtime::Handle::current();
        AsyncJoinHandle(handle.spawn(fut))
    }
}

impl<T> Future for AsyncJoinHandle<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Tokio wraps the task's return value with a `Result` that catches panics; in our case
        // we want to propagate the panic, so for once `unwrap` is the right tool to use.
        Pin::new(&mut self.0).poll(cx).map(|result| result.unwrap())
    }
}
