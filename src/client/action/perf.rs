use std::future::IntoFuture;

use futures_util::FutureExt;

use crate::{client::BoxFuture, Client};

impl Client {
    /// Add connections to the connection pool up to `min_pool_size`.  This is normally not needed -
    /// the connection pool will be filled in the background, and new connections created as needed
    /// up to `max_pool_size`.  However, it can sometimes be preferable to pay the (larger) cost of
    /// creating new connections up-front so that individual operations execute as quickly as
    /// possible.
    ///
    /// Note that topology changes require rebuilding the connection pool, so this method cannot
    /// guarantee that the pool will always be filled for the lifetime of the `Client`.
    ///
    /// Does nothing if `min_pool_size` is unset or zero.
    ///
    /// `await` will return `()`.
    pub fn warm_connection_pool(&self) -> WarmConnectionPool {
        WarmConnectionPool { client: self }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Client {
    /// Add connections to the connection pool up to `min_pool_size`.  This is normally not needed -
    /// the connection pool will be filled in the background, and new connections created as needed
    /// up to `max_pool_size`.  However, it can sometimes be preferable to pay the (larger) cost of
    /// creating new connections up-front so that individual operations execute as quickly as
    /// possible.
    ///
    /// Note that topology changes require rebuilding the connection pool, so this method cannot
    /// guarantee that the pool will always be filled for the lifetime of the `Client`.
    ///
    /// Does nothing if `min_pool_size` is unset or zero.
    ///
    /// `await` will return `()`.
    ///
    /// [`run`](WarmConnectionPool::run) will return `()`.
    pub fn warm_connection_pool(&self) -> WarmConnectionPool {
        self.async_client.warm_connection_pool()
    }
}

/// Add connections to the connection pool up to `min_pool_size`.  Create by calling
/// [`Client::warm_connection_pool`] and execute with `await` (or [`run`](WarmConnectionPool::run)
/// if using the sync client).
#[must_use]
pub struct WarmConnectionPool<'a> {
    client: &'a Client,
}

impl<'a> IntoFuture for WarmConnectionPool<'a> {
    type Output = ();
    type IntoFuture = BoxFuture<'a, ()>;

    fn into_future(self) -> Self::IntoFuture {
        async {
            if !self
                .client
                .inner
                .options
                .min_pool_size
                .map_or(false, |s| s > 0)
            {
                // No-op when min_pool_size is zero.
                return;
            }
            self.client.inner.topology.warm_pool().await;
        }
        .boxed()
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> WarmConnectionPool<'a> {
    /// Synchronously execute this action.
    pub fn run(self) {
        crate::runtime::block_on(self.into_future())
    }
}
