use std::{future::IntoFuture, sync::atomic::Ordering};

use futures_util::{future::join_all, FutureExt};

use crate::{Client, client::BoxFuture};

impl Client {
    /// Shut down this `Client`, terminating background thread workers and closing connections.  Using this method is not required under most circumstances (resources will be cleaned up in the background when dropped) but can be needed when creating `Client`s in a loop or precise control of lifespan timing is required.
    /// This will wait for any live handles to server-side resources (see below) to be
    /// dropped and any associated server-side operations to finish.
    ///
    /// IMPORTANT: Any live resource handles that are not dropped will cause this method to wait
    /// indefinitely.  It's strongly recommended to structure your usage to avoid this, e.g. by
    /// only using those types in shorter-lived scopes than the `Client`.  If this is not possible,
    /// see [`immediate`](Shutdown::immediate).  For example:
    ///
    /// ```rust
    /// # use mongodb::{Client, GridFsBucket, error::Result};
    /// async fn upload_data(bucket: &GridFsBucket) {
    ///   let stream = bucket.open_upload_stream("test", None);
    ///    // .. write to the stream ..
    /// }
    ///
    /// # async fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com").await?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// upload_data(&bucket).await;
    /// client.shutdown().await;
    /// // Background cleanup work from `upload_data` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If the handle is used in the same scope as `shutdown`, explicit `drop` may be needed:
    ///
    /// ```rust
    /// # use mongodb::{Client, error::Result};
    /// # async fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com").await?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// let stream = bucket.open_upload_stream("test", None);
    /// // .. write to the stream ..
    /// drop(stream);
    /// client.shutdown().await;
    /// // Background cleanup work for `stream` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Calling any methods on clones of this `Client` or derived handles after this will return
    /// errors.
    ///
    /// Handles to server-side resources are `Cursor`, `SessionCursor`, `Session`, or
    /// `GridFsUploadStream`.
    /// 
    /// `await` will return `()`.
    pub fn shutdown(self) -> Shutdown {
        Shutdown { client: self, immediate: false }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Client {
    /// Shut down this `Client`, terminating background thread workers and closing connections.
    /// This will wait for any live handles to server-side resources (see below) to be
    /// dropped and any associated server-side operations to finish.
    ///
    /// IMPORTANT: Any live resource handles that are not dropped will cause this method to wait
    /// indefinitely.  It's strongly recommended to structure your usage to avoid this, e.g. by
    /// only using those types in shorter-lived scopes than the `Client`.  If this is not possible,
    /// see [`shutdown_immediate`](Client::shutdown_immediate).  For example:
    ///
    /// ```rust
    /// # use mongodb::{sync::{Client, gridfs::GridFsBucket}, error::Result};
    /// fn upload_data(bucket: &GridFsBucket) {
    ///   let stream = bucket.open_upload_stream("test", None);
    ///    // .. write to the stream ..
    /// }
    ///
    /// # fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com")?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// upload_data(&bucket);
    /// client.shutdown();
    /// // Background cleanup work from `upload_data` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// If the handle is used in the same scope as `shutdown`, explicit `drop` may be needed:
    ///
    /// ```rust
    /// # use mongodb::{sync::Client, error::Result};
    /// # fn run() -> Result<()> {
    /// let client = Client::with_uri_str("mongodb://example.com")?;
    /// let bucket = client.database("test").gridfs_bucket(None);
    /// let stream = bucket.open_upload_stream("test", None);
    /// // .. write to the stream ..
    /// drop(stream);
    /// client.shutdown();
    /// // Background cleanup work for `stream` is guaranteed to have run.
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Calling any methods on  clones of this `Client` or derived handles after this will return
    /// errors.
    ///
    /// Handles to server-side resources are `Cursor`, `SessionCursor`, `Session`, or
    /// `GridFsUploadStream`.
    /// 
    /// [`run`](Shutdown::run) will return `()`.
    pub fn shutdown(self) -> Shutdown {
        self.async_client.shutdown()
    }
}

/// Shut down this `Client`, terminating background thread workers and closing connections.
#[must_use]
pub struct Shutdown {
    client: Client,
    immediate: bool,
}

impl Shutdown {
    /// If `true`, execution will not wait for pending resources to be cleaned up,
    /// which may cause both client-side errors and server-side resource leaks.  Defaults to `false`.
    pub fn immediate(mut self, value: bool) -> Self {
        self.immediate = value;
        self
    }
}

impl IntoFuture for Shutdown {
    type Output = ();

    type IntoFuture = BoxFuture<'static, ()>;

    fn into_future(self) -> Self::IntoFuture {
        async move {
            if !self.immediate {
                // Subtle bug: if this is inlined into the `join_all(..)` call, Rust will extend the
                // lifetime of the temporary unnamed `MutexLock` until the end of the *statement*,
                // causing the lock to be held for the duration of the join, which deadlocks.
                let pending = self.client.inner.shutdown.pending_drops.lock().unwrap().extract();
                join_all(pending).await;
            }
            self.client.inner.topology.shutdown().await;
            // This has to happen last to allow pending cleanup to execute commands.
            self.client.inner.shutdown.executed.store(true, Ordering::SeqCst);
        }.boxed()
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl Shutdown {
    /// Synchronously execute this action.
    pub fn run(self) {
        crate::runtime::block_on(self.into_future())
    }
}