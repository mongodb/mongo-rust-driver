// TODO RUST-212: Remove annotation.
#[allow(dead_code)]
mod stream;
mod join_handle;

use std::future::Future;
use std::time::Duration;

use async_trait::async_trait;
use futures_timer::Delay;
use futures::future::{self, Either};

use self::stream::AsyncStream;
use crate::{cmap::conn::StreamOptions, error::{Error, Result}};
pub(crate) use self::join_handle::AsyncJoinHandle;

/// An abstract handle to the async runtime.
#[derive(Clone, Copy, Debug)]
pub(crate) enum AsyncRuntime {
    /// Represents the `tokio` runtime.
    #[cfg(feature = "tokio-runtime")]
    Tokio,

    /// Represents the `async-std` runtime.
    #[cfg(feature = "async-std-runtime")]
    AsyncStd,
}

impl AsyncRuntime {
    /// Spawn a task in the background to run a future.
    pub(crate) fn execute<F, O>(self, fut: F) -> AsyncJoinHandle<O>
    where
        F: Future<Output = O> + Send + 'static,
        O: Send + 'static
    {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio => {
                AsyncJoinHandle::Tokio(tokio::task::spawn(fut))
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd => {
                AsyncJoinHandle::AsyncStd(async_std::task::spawn(fut))
            }
        }
    }

    /// Run a future in the foreground, blocking on it completing.
    pub(crate) fn block_on<F, T>(self, fut: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        #[cfg(all(feature = "tokio-runtime", not(feature = "async-std-runtime")))]
        {
            use tokio::runtime::Handle;

            Handle::current().enter(|| futures::executor::block_on(fut))
        }

        #[cfg(feature = "async-std-runtime")]
        {
            async_std::task::block_on(fut)
        }
    }

    /// Create and connect a new `AsyncStream`.
    // RUST-212: Remove annotation.
    #[allow(dead_code)]
    pub(crate) async fn connect_stream(self, options: StreamOptions) -> Result<AsyncStream> {
        AsyncStream::connect(options).await
    }
}

#[async_trait]
pub(crate) trait JoinHandle {
    type Output;

    
}

#[async_trait]
pub(crate) trait Timeoutable {
    type Output;
    
    async fn with_timeout<F: Send + Fn() -> Error>(self, timeout: Duration, error_func: F) -> Result<Self::Output>;
}

#[async_trait]
impl<T: Future + Unpin + Send> Timeoutable for T {
    type Output = T::Output;

    async fn with_timeout<F: Send + Fn() -> Error>(self, timeout: Duration, error_func: F) -> Result<T::Output> {
        match future::select(self, Delay::new(timeout)).await {
            Either::Left((result, _)) => Ok(result),
            Either::Right(_) => Err(error_func())
        }
    }
}
