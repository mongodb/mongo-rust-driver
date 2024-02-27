use std::{
    future::Future,
    ops::DerefMut,
    pin::Pin,
    task::{Context, Poll},
};

/// A runtime-agnostic handle used for awaiting on tasks spawned in `AsyncRuntime::execute`.
/// Wraps either `tokio::task::JoinHandle` or `async_std::task::JoinHandle`.
///
/// Note: the `Future::Output` of this handle is `Result<T>`, not just `T`.
#[derive(Debug)]
pub(crate) enum AsyncJoinHandle<T> {
    /// Wrapper around `tokio::task:JoinHandle`.
    Tokio(tokio::task::JoinHandle<T>),
}

impl<T> Future for AsyncJoinHandle<T> {
    // tokio wraps the Output of its JoinHandle in a Result that contains an error if the task
    // panicked, while async-std does not.
    //
    // Given that async-std will panic or abort the task in this scenario, there is not a
    // lot of value in preserving the error in tokio.
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.deref_mut() {
            Self::Tokio(ref mut handle) => Pin::new(handle).poll(cx).map(|result| result.unwrap()),
        }
    }
}
