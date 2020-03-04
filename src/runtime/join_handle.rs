use std::{
    ops::DerefMut,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use crate::error::Result;

/// A runtime-agnostic handle used for awaiting on tasks spawned in `AsyncRuntime::execute`.
/// Wraps either `tokio::task::JoinHandle` or `async_std::task::JoinHandle`.
///
/// Note: the `Future::Output` of this handle is `Result<T>`, not just `T`.
#[derive(Debug)]
pub(crate) enum AsyncJoinHandle<T> {
    /// Wrapper around `tokio::task:JoinHandle`.
    #[cfg(feature = "tokio-runtime")]
    Tokio(tokio::task::JoinHandle<T>),

    /// Wrapper around `tokio::task:JoinHandle`.
    #[cfg(feature = "async-std-runtime")]
    AsyncStd(async_std::task::JoinHandle<T>),
}

impl<T> Future for AsyncJoinHandle<T> {
    // tokio wraps the Output of its JoinHandle in a Result, while async-std does not.
    // In order to standardize on this without us panicking, we too wrap the output in a Result.
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.deref_mut() {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio(ref mut handle) => {
                use crate::error::ErrorKind;

                Pin::new(handle).poll(cx)
                    .map(|result| {
                        result.map_err(|e| ErrorKind::InternalError { message: format!("{}", e) }.into())
                    })
            },

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd(ref mut handle) => {
                Pin::new(handle).poll(cx).map(Ok)
            }
        }
    }
}
