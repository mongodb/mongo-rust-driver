mod join_handle;
mod stream;

use std::{future::Future, time::Duration};

use futures::future::{self, Either};
use futures_timer::Delay;

pub(crate) use self::{join_handle::AsyncJoinHandle, stream::AsyncStream};
use crate::error::{ErrorKind, Result};

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
    ///
    /// If the runtime is still running, this will return a handle to the background task.
    /// Otherwise, it will return `None`.
    pub(crate) fn spawn<F, O>(self, fut: F) -> Option<AsyncJoinHandle<O>>
    where
        F: Future<Output = O> + Send + 'static,
        O: Send + 'static,
    {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio => {
                use tokio::runtime::Handle;

                let handle = Handle::try_current().ok()?;

                Some(AsyncJoinHandle::Tokio(handle.spawn(fut)))
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd => Some(AsyncJoinHandle::AsyncStd(async_std::task::spawn(fut))),
        }
    }

    /// Spawn a task in the background to run a future.
    pub(crate) fn execute<F, O>(self, fut: F)
    where
        F: Future<Output = O> + Send + 'static,
        O: Send + 'static,
    {
        self.spawn(fut);
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

    /// Await on a future for a maximum amount of time before returning an error.
    pub(crate) async fn await_with_timeout<F>(
        self,
        future: F,
        timeout: Duration,
    ) -> Result<F::Output>
    where
        F: Future + Send + Unpin,
    {
        match future::select(future, Delay::new(timeout)).await {
            Either::Left((result, _)) => Ok(result),
            Either::Right(_) => Err(ErrorKind::InternalError {
                message: "Timed out waiting on future".to_string(),
            }
            .into()),
        }
    }
}
