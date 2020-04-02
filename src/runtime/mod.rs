mod async_read_ext;
mod async_write_ext;
mod join_handle;
mod resolver;
mod stream;

use std::{future::Future, net::SocketAddr, time::Duration};

pub(crate) use self::{
    async_read_ext::AsyncLittleEndianRead,
    async_write_ext::AsyncLittleEndianWrite,
    join_handle::AsyncJoinHandle,
    resolver::AsyncResolver,
    stream::AsyncStream,
};
use crate::{
    error::{ErrorKind, Result},
    options::StreamAddress,
};

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
    #[cfg(test)]
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

    /// Delay for the specified duration.
    pub(crate) async fn delay_for(self, delay: Duration) {
        #[cfg(feature = "tokio-runtime")]
        {
            tokio::time::delay_for(delay).await
        }

        #[cfg(feature = "async-std-runtime")]
        {
            async_std::task::sleep(delay).await
        }
    }

    /// Await on a future for a maximum amount of time before returning an error.
    pub(crate) async fn timeout<F: Future>(
        self,
        timeout: Duration,
        future: F,
    ) -> Result<F::Output> {
        #[cfg(feature = "tokio-runtime")]
        {
            tokio::time::timeout(timeout, future)
                .await
                .map_err(|e| ErrorKind::Io(e.into()).into())
        }

        #[cfg(feature = "async-std-runtime")]
        {
            async_std::future::timeout(timeout, future)
                .await
                .map_err(|_| ErrorKind::Io(std::io::ErrorKind::TimedOut.into()).into())
        }
    }

    pub(crate) async fn resolve_address(
        self,
        address: &StreamAddress,
    ) -> Result<impl Iterator<Item = SocketAddr>> {
        match self {
            #[cfg(feature = "tokio-runtime")]
            Self::Tokio => {
                let socket_addrs = tokio::net::lookup_host(format!("{}", address)).await?;
                Ok(socket_addrs)
            }

            #[cfg(feature = "async-std-runtime")]
            Self::AsyncStd => {
                let host = (address.hostname.as_str(), address.port.unwrap_or(27017));
                let socket_addrs = async_std::net::ToSocketAddrs::to_socket_addrs(&host).await?;
                Ok(socket_addrs)
            }
        }
    }
}
