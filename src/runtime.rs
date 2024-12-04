mod acknowledged_message;
#[cfg(feature = "reqwest")]
mod http;
#[cfg(feature = "async-std-runtime")]
mod interval;
mod join_handle;
#[cfg(feature = "cert-key-password")]
mod pem;
#[cfg(any(
    feature = "in-use-encryption-unstable",
    all(test, not(feature = "sync"), not(feature = "tokio-sync"))
))]
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
#[cfg(feature = "async-std-runtime")]
use interval::Interval;
#[cfg(feature = "openssl-tls")]
use tls_openssl as tls;
#[cfg(not(feature = "openssl-tls"))]
use tls_rustls as tls;
#[cfg(feature = "tokio-runtime")]
use tokio::time::Interval;

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

/// Delay for the specified duration.
pub(crate) async fn delay_for(delay: Duration) {
    #[cfg(feature = "tokio-runtime")]
    {
        tokio::time::sleep(delay).await
    }

    #[cfg(feature = "async-std-runtime")]
    {
        // This avoids a panic in async-std when the provided duration is too large.
        // See: https://github.com/async-rs/async-std/issues/1037.
        if delay == Duration::MAX {
            std::future::pending().await
        } else {
            async_std::task::sleep(delay).await
        }
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
        // This avoids a panic on async-std when the provided duration is too large.
        // See: https://github.com/async-rs/async-std/issues/1037.
        if timeout == Duration::MAX {
            Ok(future.await)
        } else {
            async_std::future::timeout(timeout, future)
                .await
                .map_err(|_| std::io::ErrorKind::TimedOut.into())
        }
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
        use async_std::net::ToSocketAddrs;

        let socket_addrs = format!("{}", address).to_socket_addrs().await?;
        Ok(socket_addrs)
    }
}
