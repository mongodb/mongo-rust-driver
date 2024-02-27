mod acknowledged_message;
#[cfg(feature = "reqwest")]
mod http;
mod join_handle;
#[cfg(any(feature = "in-use-encryption-unstable", test))]
pub(crate) mod process;
mod resolver;
pub(crate) mod stream;
mod sync_read_ext;
#[cfg(feature = "openssl-tls")]
mod tls_openssl;
#[cfg_attr(feature = "openssl-tls", allow(unused))]
mod tls_rustls;
mod worker_handle;

use std::{future::Future, net::SocketAddr, time::Duration};

pub(crate) use self::{
    acknowledged_message::{AcknowledgedMessage, AcknowledgmentReceiver, AcknowledgmentSender},
    join_handle::AsyncJoinHandle,
    resolver::AsyncResolver,
    stream::AsyncStream,
    sync_read_ext::SyncLittleEndianRead,
    worker_handle::{WorkerHandle, WorkerHandleListener},
};
use crate::{error::Result, options::ServerAddress};
#[cfg(feature = "reqwest")]
pub(crate) use http::HttpClient;
#[cfg(feature = "openssl-tls")]
use tls_openssl as tls;
#[cfg(not(feature = "openssl-tls"))]
use tls_rustls as tls;

pub(crate) use tls::TlsConfig;

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
    #[cfg(not(feature = "sync"))]
    {
        let handle = tokio::runtime::Handle::current();
        AsyncJoinHandle::Tokio(handle.spawn(fut))
    }

    #[cfg(feature = "sync")]
    {
        let handle = crate::sync::TOKIO_RUNTIME.handle();
        AsyncJoinHandle::Tokio(handle.spawn(fut))
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

#[cfg(any(test, feature = "sync"))]
pub(crate) fn block_on<F, T>(fut: F) -> T
where
    F: Future<Output = T>,
{
    #[cfg(not(feature = "sync"))]
    {
        tokio::task::block_in_place(|| futures::executor::block_on(fut))
    }

    #[cfg(feature = "sync")]
    {
        crate::sync::TOKIO_RUNTIME.block_on(fut)
    }
}

/// Delay for the specified duration.
pub(crate) async fn delay_for(delay: Duration) {
    tokio::time::sleep(delay).await
}

/// Await on a future for a maximum amount of time before returning an error.
pub(crate) async fn timeout<F: Future>(timeout: Duration, future: F) -> Result<F::Output> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| std::io::ErrorKind::TimedOut.into())
}

/// Create a new `Interval` that yields with interval of `duration`.
/// See: <https://docs.rs/tokio/latest/tokio/time/fn.interval.html>
pub(crate) fn interval(duration: Duration) -> tokio::time::Interval {
    tokio::time::interval(duration)
}

pub(crate) async fn resolve_address(
    address: &ServerAddress,
) -> Result<impl Iterator<Item = SocketAddr>> {
    let socket_addrs = tokio::net::lookup_host(format!("{}", address)).await?;
    Ok(socket_addrs)
}
