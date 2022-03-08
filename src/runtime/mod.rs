mod acknowledged_message;
mod async_read_ext;
mod async_write_ext;
mod http;
#[cfg(feature = "async-std-runtime")]
mod interval;
mod join_handle;
mod resolver;
mod stream;

use std::{future::Future, net::SocketAddr, time::Duration};

pub(crate) use self::{
    acknowledged_message::AcknowledgedMessage,
    async_read_ext::{AsyncLittleEndianRead, SyncLittleEndianRead},
    async_write_ext::{AsyncLittleEndianWrite, SyncLittleEndianWrite},
    join_handle::AsyncJoinHandle,
    resolver::AsyncResolver,
    stream::AsyncStream,
};
use crate::{error::Result, options::ServerAddress};
pub(crate) use http::HttpClient;
#[cfg(feature = "async-std-runtime")]
use interval::Interval;
#[cfg(feature = "tokio-runtime")]
use tokio::time::Interval;

/// Spawn a task in the background to run a future.
///
/// If the runtime is still running, this will return a handle to the background task.
/// Otherwise, it will return `None`. As a result, this must be called from an async block
/// or function running on a runtime.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn spawn<F, O>(fut: F) -> AsyncJoinHandle<O>
where
    F: Future<Output = O> + Send + 'static,
    O: Send + 'static,
{
    #[cfg(all(feature = "tokio-runtime", not(feature = "tokio-sync")))]
    {
        let handle = tokio::runtime::Handle::current();
        AsyncJoinHandle::Tokio(handle.spawn(fut))
    }

    #[cfg(feature = "tokio-sync")]
    {
        let handle = crate::sync::TOKIO_RUNTIME.handle();
        AsyncJoinHandle::Tokio(handle.spawn(fut))
    }

    #[cfg(feature = "async-std-runtime")]
    {
        AsyncJoinHandle::AsyncStd(async_std::task::spawn(fut))
    }
}

/// Spawn a task in the background to run a future.
///
/// Note: this must only be called from an async block or function running on a runtime.
pub(crate) fn execute<F, O>(fut: F)
where
    F: Future<Output = O> + Send + 'static,
    O: Send + 'static,
{
    spawn(fut);
}

#[cfg(any(test, feature = "sync", feature = "tokio-sync"))]
pub(crate) fn block_on<F, T>(fut: F) -> T
where
    F: Future<Output = T>,
{
    #[cfg(all(feature = "tokio-runtime", not(feature = "tokio-sync")))]
    {
        tokio::task::block_in_place(|| futures::executor::block_on(fut))
    }

    #[cfg(feature = "tokio-sync")]
    {
        crate::sync::TOKIO_RUNTIME.block_on(fut)
    }

    #[cfg(feature = "async-std-runtime")]
    {
        async_std::task::block_on(fut)
    }
}

/// Run a future in the foreground, blocking on it completing.
/// This does not notify the runtime that it will be blocking and should only be used for
/// operations that will immediately (or quickly) succeed.
pub(crate) fn block_in_place<F, T>(fut: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    futures_executor::block_on(fut)
}

/// Delay for the specified duration.
pub(crate) async fn delay_for(delay: Duration) {
    #[cfg(feature = "tokio-runtime")]
    {
        tokio::time::sleep(delay).await
    }

    #[cfg(feature = "async-std-runtime")]
    {
        async_std::task::sleep(delay).await
    }
}

/// Await on a future for a maximum amount of time before returning an error.
pub(crate) async fn timeout<F: Future>(timeout: Duration, future: F) -> Result<F::Output> {
    #[cfg(feature = "tokio-runtime")]
    {
        tokio::time::timeout(timeout, future)
            .await
            .map_err(|_| std::io::ErrorKind::TimedOut.into())
    }

    #[cfg(feature = "async-std-runtime")]
    {
        async_std::future::timeout(timeout, future)
            .await
            .map_err(|_| std::io::ErrorKind::TimedOut.into())
    }
}

/// Create a new `Interval` that yields with interval of `duration`.
/// See: <https://docs.rs/tokio/latest/tokio/time/fn.interval.html>
pub(crate) fn interval(duration: Duration) -> Interval {
    #[cfg(feature = "tokio-runtime")]
    {
        tokio::time::interval(duration)
    }

    #[cfg(feature = "async-std-runtime")]
    {
        Interval::new(duration)
    }
}

pub(crate) async fn resolve_address(
    address: &ServerAddress,
) -> Result<impl Iterator<Item = SocketAddr>> {
    #[cfg(feature = "tokio-runtime")]
    {
        let socket_addrs = tokio::net::lookup_host(format!("{}", address)).await?;
        Ok(socket_addrs)
    }

    #[cfg(feature = "async-std-runtime")]
    {
        let host = (address.host(), address.port().unwrap_or(27017));
        let socket_addrs = async_std::net::ToSocketAddrs::to_socket_addrs(&host).await?;
        Ok(socket_addrs)
    }
}
