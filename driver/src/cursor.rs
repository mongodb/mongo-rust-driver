mod common;
pub(crate) mod raw_batch;
pub(crate) mod session;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::bson::RawDocument;

#[cfg(test)]
use crate::bson::RawDocumentBuf;
use derive_where::derive_where;
use futures_core::Stream;
use serde::{de::DeserializeOwned, Deserialize};
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    change_stream::event::ResumeToken,
    client::{options::ServerAddress, AsyncDropToken},
    cmap::conn::PinnedConnectionHandle,
    cursor::common::ImplicitClientSessionHandle,
    error::{Error, Result},
    Client,
    ClientSession,
};
use common::{kill_cursor, GenericCursor};
pub(crate) use common::{
    reply_batch,
    stream_poll_next,
    BatchValue,
    CursorInformation,
    CursorSpecification,
    CursorSpecification2,
    CursorStream,
    NextInBatchFuture,
    PinnedConnection,
};

/// A [`Cursor`] streams the result of a query. When a query is made, the returned [`Cursor`] will
/// contain the first batch of results from the server; the individual results will then be returned
/// as the [`Cursor`] is iterated. When the batch is exhausted and if there are more results, the
/// [`Cursor`] will fetch the next batch of documents, and so forth until the results are exhausted.
/// Note that because of this batching, additional network I/O may occur on any given call to
/// `next`. Because of this, a [`Cursor`] iterates over `Result<T>` items rather than
/// simply `T` items.
///
/// The batch size of the `Cursor` can be configured using the options to the method that returns
/// it. For example, setting the `batch_size` field of
/// [`FindOptions`](options/struct.FindOptions.html) will set the batch size of the
/// `Cursor` returned by [`Collection::find`](struct.Collection.html#method.find).
///
/// Note that the batch size determines both the number of documents stored in memory by the
/// `Cursor` at a given time as well as the total number of network round-trips needed to fetch all
/// results from the server; both of these factors should be taken into account when choosing the
/// optimal batch size.
///
/// [`Cursor`] implements [`Stream`](https://docs.rs/futures/latest/futures/stream/trait.Stream.html), which means
/// it can be iterated over much in the same way that an `Iterator` can be in synchronous Rust. In
/// order to do so, the [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html) trait must
/// be imported. Because a [`Cursor`] iterates over a `Result<T>`, it also has access to the
/// potentially more ergonomic functionality provided by
/// [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html), which can be
/// imported instead of or in addition to
/// [`StreamExt`](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html). The methods from
/// [`TryStreamExt`](https://docs.rs/futures/latest/futures/stream/trait.TryStreamExt.html) are especially useful when
/// used in conjunction with the `?` operator.
///
/// ```rust
/// # use mongodb::{bson::{Document, doc}, Client, error::Result};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// #
/// use futures::stream::{StreamExt, TryStreamExt};
///
/// let mut cursor = coll.find(doc! {}).await?;
/// // regular Stream uses next() and iterates over Option<Result<T>>
/// while let Some(doc) = cursor.next().await {
///   println!("{}", doc?)
/// }
/// // regular Stream uses collect() and collects into a Vec<Result<T>>
/// let v: Vec<Result<_>> = cursor.collect().await;
///
/// let mut cursor = coll.find(doc! {}).await?;
/// // TryStream uses try_next() and iterates over Result<Option<T>>
/// while let Some(doc) = cursor.try_next().await? {
///   println!("{}", doc)
/// }
/// // TryStream uses try_collect() and collects into a Result<Vec<T>>
/// let v: Vec<_> = cursor.try_collect().await?;
/// #
/// # Ok(())
/// # }
/// ```
///
/// If a [`Cursor`] is still open when it goes out of scope, it will automatically be closed via an
/// asynchronous [killCursors](https://www.mongodb.com/docs/manual/reference/command/killCursors/) command executed
/// from its [`Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html) implementation.
#[derive_where(Debug)]
pub struct Cursor<T> {
    client: Client,
    drop_token: AsyncDropToken,
    // `wrapped_cursor` is an `Option` so that it can be `None` for the `drop` impl for a cursor
    // that's had `with_type` called; in all other circumstances it will be `Some`.
    wrapped_cursor: Option<ImplicitSessionCursor>,
    drop_address: Option<ServerAddress>,
    #[cfg(test)]
    kill_watcher: Option<oneshot::Sender<()>>,
    #[derive_where(skip)]
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T> Cursor<T> {
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification,
        session: Option<ClientSession>,
        pin: Option<PinnedConnectionHandle>,
    ) -> Self {
        Self {
            client: client.clone(),
            drop_token: client.register_async_drop(),
            wrapped_cursor: Some(ImplicitSessionCursor::with_implicit_session(
                client,
                spec,
                PinnedConnection::new(pin),
                ImplicitClientSessionHandle(session),
            )),
            drop_address: None,
            #[cfg(test)]
            kill_watcher: None,
            _phantom: Default::default(),
        }
    }

    pub(crate) fn new2(
        client: Client,
        spec: CursorSpecification2,
        session: Option<ClientSession>,
        pin: Option<PinnedConnectionHandle>,
    ) -> Result<Self> {
        Ok(Self {
            client: client.clone(),
            drop_token: client.register_async_drop(),
            wrapped_cursor: Some(ImplicitSessionCursor::with_implicit_session2(
                client,
                spec,
                PinnedConnection::new(pin),
                ImplicitClientSessionHandle(session),
            )?),
            drop_address: None,
            #[cfg(test)]
            kill_watcher: None,
            _phantom: Default::default(),
        })
    }

    pub(crate) fn post_batch_resume_token(&self) -> Option<&ResumeToken> {
        self.wrapped_cursor
            .as_ref()
            .and_then(|c| c.post_batch_resume_token())
    }

    pub(crate) fn client(&self) -> &Client {
        &self.client
    }

    pub(crate) fn address(&self) -> &ServerAddress {
        self.wrapped_cursor.as_ref().unwrap().address()
    }

    pub(crate) fn set_drop_address(&mut self, address: ServerAddress) {
        self.drop_address = Some(address);
    }

    pub(crate) fn take_implicit_session(&mut self) -> Option<ClientSession> {
        self.wrapped_cursor
            .as_mut()
            .and_then(|c| c.take_implicit_session())
    }

    /// Move the cursor forward, potentially triggering requests to the database for more results
    /// if the local buffer has been exhausted.
    ///
    /// This will keep requesting data from the server until either the cursor is exhausted
    /// or batch with results in it has been received.
    ///
    /// The return value indicates whether new results were successfully returned (true) or if
    /// the cursor has been closed (false).
    ///
    /// Note: [`Cursor::current`] and [`Cursor::deserialize_current`] must only be called after
    /// [`Cursor::advance`] returned `Ok(true)`. It is an error to call either of them without
    /// calling [`Cursor::advance`] first or after [`Cursor::advance`] returns an error / false.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{Document, doc}, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let coll = client.database("stuff").collection::<Document>("stuff");
    /// let mut cursor = coll.find(doc! {}).await?;
    /// while cursor.advance().await? {
    ///     println!("{:?}", cursor.current());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn advance(&mut self) -> Result<bool> {
        self.wrapped_cursor.as_mut().unwrap().advance().await
    }

    #[cfg(test)]
    pub(crate) async fn try_advance(&mut self) -> Result<()> {
        self.wrapped_cursor
            .as_mut()
            .unwrap()
            .try_advance()
            .await
            .map(|_| ())
    }

    /// Returns a reference to the current result in the cursor.
    ///
    /// # Panics
    /// [`Cursor::advance`] must return `Ok(true)` before [`Cursor::current`] can be
    /// invoked. Calling [`Cursor::current`] after [`Cursor::advance`] does not return true
    /// or without calling [`Cursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{Document, doc}, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let coll = client.database("stuff").collection::<Document>("stuff");
    /// let mut cursor = coll.find(doc! {}).await?;
    /// while cursor.advance().await? {
    ///     println!("{:?}", cursor.current());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn current(&self) -> &RawDocument {
        self.wrapped_cursor.as_ref().unwrap().current().unwrap()
    }

    /// Whether this cursor has exhausted all of its getMore calls. The cursor may have more
    /// items remaining in the buffer.
    pub(crate) fn is_exhausted(&self) -> bool {
        self.wrapped_cursor.as_ref().unwrap().is_exhausted()
    }

    /// Returns true if the cursor has any additional items to return and false otherwise.
    pub fn has_next(&self) -> bool {
        !self.is_exhausted()
            || !self
                .wrapped_cursor
                .as_ref()
                .unwrap()
                .state()
                .buffer
                .is_empty()
    }
    /// Deserialize the current result to the generic type associated with this cursor.
    ///
    /// # Panics
    /// [`Cursor::advance`] must return `Ok(true)` before [`Cursor::deserialize_current`] can be
    /// invoked. Calling [`Cursor::deserialize_current`] after [`Cursor::advance`] does not return
    /// true or without calling [`Cursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, error::Result, bson::doc};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let db = client.database("foo");
    /// use serde::Deserialize;
    ///
    /// #[derive(Debug, Deserialize)]
    /// struct Cat<'a> {
    ///     #[serde(borrow)]
    ///     name: &'a str
    /// }
    ///
    /// let coll = db.collection::<Cat>("cat");
    /// let mut cursor = coll.find(doc! {}).await?;
    /// while cursor.advance().await? {
    ///     println!("{:?}", cursor.deserialize_current()?);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn deserialize_current<'a>(&'a self) -> Result<T>
    where
        T: Deserialize<'a>,
    {
        crate::bson_compat::deserialize_from_slice(self.current().as_bytes()).map_err(Error::from)
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<'a, D>(mut self) -> Cursor<D>
    where
        D: Deserialize<'a>,
    {
        Cursor {
            client: self.client.clone(),
            drop_token: self.drop_token.take(),
            wrapped_cursor: self.wrapped_cursor.take(),
            drop_address: self.drop_address.take(),
            #[cfg(test)]
            kill_watcher: self.kill_watcher.take(),
            _phantom: Default::default(),
        }
    }

    /// Some tests need to be able to observe the events generated by `killCommand` execution;
    /// however, because that happens asynchronously on `drop`, the test runner can conclude before
    /// the event is published.  To fix that, tests can set a "kill watcher" on cursors - a
    /// one-shot channel with a `()` value pushed after `killCommand` is run that the test can wait
    /// on.
    #[cfg(test)]
    pub(crate) fn set_kill_watcher(&mut self, tx: oneshot::Sender<()>) {
        assert!(
            self.kill_watcher.is_none(),
            "cursor already has a kill_watcher"
        );
        self.kill_watcher = Some(tx);
    }

    #[cfg(test)]
    pub(crate) fn current_batch(&self) -> &std::collections::VecDeque<RawDocumentBuf> {
        self.wrapped_cursor.as_ref().unwrap().current_batch()
    }
}

impl<T> CursorStream for Cursor<T>
where
    T: DeserializeOwned,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        self.wrapped_cursor.as_mut().unwrap().poll_next_in_batch(cx)
    }
}

impl<T> Stream for Cursor<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // This `unwrap` is safe because `wrapped_cursor` is always `Some` outside of `drop`.
        stream_poll_next(self.wrapped_cursor.as_mut().unwrap(), cx)
    }
}

impl<T> Drop for Cursor<T> {
    fn drop(&mut self) {
        let wrapped_cursor = match &self.wrapped_cursor {
            None => return,
            Some(c) => c,
        };
        if wrapped_cursor.is_exhausted() {
            return;
        }

        kill_cursor(
            self.client.clone(),
            &mut self.drop_token,
            wrapped_cursor.namespace(),
            wrapped_cursor.id(),
            wrapped_cursor.pinned_connection().replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            self.kill_watcher.take(),
        );
    }
}

/// A `GenericCursor` that optionally owns its own sessions.
/// This is to be used by cursors associated with implicit sessions.
type ImplicitSessionCursor = GenericCursor<'static, ImplicitClientSessionHandle>;
