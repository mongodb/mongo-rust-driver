use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::{bson::RawDocument, cursor::CursorSpecification2};
use futures_core::Stream;
use futures_util::StreamExt;
use serde::{de::DeserializeOwned, Deserialize};
#[cfg(test)]
use tokio::sync::oneshot;

use super::{
    common::{
        kill_cursor,
        CursorBuffer,
        CursorInformation,
        CursorState,
        GenericCursor,
        PinnedConnection,
    },
    stream_poll_next,
    BatchValue,
    CursorStream,
};
use crate::{
    bson::Document,
    change_stream::event::ResumeToken,
    client::{options::ServerAddress, AsyncDropToken},
    cmap::conn::PinnedConnectionHandle,
    cursor::{common::ExplicitClientSessionHandle, CursorSpecification},
    error::{Error, Result},
    Client,
    ClientSession,
};

/// A [`SessionCursor`] is a cursor that was created with a [`ClientSession`] that must be iterated
/// using one. To iterate, use [`SessionCursor::next`] or retrieve a [`SessionCursorStream`] using
/// [`SessionCursor::stream`]:
///
/// ```rust
/// # use mongodb::{bson::{Document, doc}, Client, error::Result, ClientSession, SessionCursor};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let mut session = client.start_session().await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// #
/// // iterate using next()
/// let mut cursor = coll.find(doc! {}).session(&mut session).await?;
/// while let Some(doc) = cursor.next(&mut session).await.transpose()? {
///     println!("{}", doc)
/// }
///
/// // iterate using `Stream`:
/// use futures::stream::TryStreamExt;
///
/// let mut cursor = coll.find(doc! {}).session(&mut session).await?;
/// let results: Vec<_> = cursor.stream(&mut session).try_collect().await?;
/// #
/// # Ok(())
/// # }
/// ```
///
/// If a [`SessionCursor`] is still open when it goes out of scope, it will automatically be closed
/// via an asynchronous [killCursors](https://www.mongodb.com/docs/manual/reference/command/killCursors/) command executed
/// from its [`Drop`](https://doc.rust-lang.org/std/ops/trait.Drop.html) implementation.
#[derive(Debug)]
pub struct SessionCursor<T> {
    client: Client,
    drop_token: AsyncDropToken,
    info: CursorInformation,
    state: Option<CursorState>,
    drop_address: Option<ServerAddress>,
    _phantom: PhantomData<T>,
    #[cfg(test)]
    kill_watcher: Option<oneshot::Sender<()>>,
}

impl<T> SessionCursor<T> {
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification,
        pinned: Option<PinnedConnectionHandle>,
    ) -> Self {
        let exhausted = spec.info.id == 0;

        Self {
            drop_token: client.register_async_drop(),
            client,
            info: spec.info,
            drop_address: None,
            _phantom: Default::default(),
            #[cfg(test)]
            kill_watcher: None,
            state: CursorState {
                buffer: CursorBuffer::new(spec.initial_buffer),
                exhausted,
                post_batch_resume_token: None,
                pinned_connection: PinnedConnection::new(pinned),
            }
            .into(),
        }
    }

    pub(crate) fn new2(
        client: Client,
        spec: CursorSpecification2,
        pinned: Option<PinnedConnectionHandle>,
    ) -> Result<Self> {
        let exhausted = spec.info.id == 0;

        Ok(Self {
            drop_token: client.register_async_drop(),
            client,
            info: spec.info,
            drop_address: None,
            _phantom: Default::default(),
            #[cfg(test)]
            kill_watcher: None,
            state: CursorState {
                buffer: CursorBuffer::new(super::common::reply_batch(&spec.initial_reply)?),
                exhausted,
                post_batch_resume_token: None,
                pinned_connection: PinnedConnection::new(pinned),
            }
            .into(),
        })
    }
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned,
{
    /// Retrieves a [`SessionCursorStream`] to iterate this cursor. The session provided must be the
    /// same session used to create the cursor.
    ///
    /// Note that the borrow checker will not allow the session to be reused in between iterations
    /// of this stream. In order to do that, either use [`SessionCursor::next`] instead or drop
    /// the stream before using the session.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{doc, Document}, error::Result};
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session().await?;
    /// #
    /// use futures::stream::TryStreamExt;
    ///
    /// // iterate over the results
    /// let mut cursor = coll.find(doc! { "x": 1 }).session(&mut session).await?;
    /// while let Some(doc) = cursor.stream(&mut session).try_next().await? {
    ///     println!("{}", doc);
    /// }
    ///
    /// // collect the results
    /// let mut cursor1 = coll.find(doc! { "x": 1 }).session(&mut session).await?;
    /// let v: Vec<Document> = cursor1.stream(&mut session).try_collect().await?;
    ///
    /// // use session between iterations
    /// let mut cursor2 = coll.find(doc! { "x": 1 }).session(&mut session).await?;
    /// loop {
    ///     let doc = match cursor2.stream(&mut session).try_next().await? {
    ///         Some(d) => d,
    ///         None => break,
    ///     };
    ///     other_coll.insert_one(doc).session(&mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub fn stream<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorStream<'_, 'session, T> {
        self.make_stream(session)
    }

    /// Retrieve the next result from the cursor.
    /// The session provided must be the same session used to create the cursor.
    ///
    /// Use this method when the session needs to be used again between iterations or when the added
    /// functionality of `Stream` is not needed.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{doc, Document}};
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session().await?;
    /// let mut cursor = coll.find(doc! { "x": 1 }).session(&mut session).await?;
    /// while let Some(doc) = cursor.next(&mut session).await.transpose()? {
    ///     other_coll.insert_one(doc).session(&mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub async fn next(&mut self, session: &mut ClientSession) -> Option<Result<T>> {
        self.stream(session).next().await
    }
}

impl<T> SessionCursor<T> {
    fn make_stream<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorStream<'_, 'session, T> {
        // Pass the state into this cursor handle for iteration.
        // It will be returned in the handle's `Drop` implementation.
        SessionCursorStream {
            generic_cursor: ExplicitSessionCursor::with_explicit_session(
                self.take_state(),
                self.client.clone(),
                self.info.clone(),
                ExplicitClientSessionHandle(session),
            ),
            session_cursor: self,
        }
    }

    fn take_state(&mut self) -> CursorState {
        self.state.take().unwrap()
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
    /// Note: [`SessionCursor::current`] and [`SessionCursor::deserialize_current`] must only be
    /// called after [`SessionCursor::advance`] returned `Ok(true)`. It is an error to call
    /// either of them without calling [`SessionCursor::advance`] first or after
    /// [`SessionCursor::advance`] returns an error / false.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{doc, Document}, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let mut session = client.start_session().await?;
    /// # let coll = client.database("stuff").collection::<Document>("stuff");
    /// let mut cursor = coll.find(doc! {}).session(&mut session).await?;
    /// while cursor.advance(&mut session).await? {
    ///     println!("{:?}", cursor.current());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn advance(&mut self, session: &mut ClientSession) -> Result<bool> {
        self.make_stream(session).generic_cursor.advance().await
    }

    #[cfg(test)]
    pub(crate) async fn try_advance(&mut self, session: &mut ClientSession) -> Result<()> {
        self.make_stream(session)
            .generic_cursor
            .try_advance()
            .await
            .map(|_| ())
    }

    /// Returns a reference to the current result in the cursor.
    ///
    /// # Panics
    /// [`SessionCursor::advance`] must return `Ok(true)` before [`SessionCursor::current`] can be
    /// invoked. Calling [`SessionCursor::current`] after [`SessionCursor::advance`] does not return
    /// true or without calling [`SessionCursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, bson::{Document, doc}, error::Result};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let mut session = client.start_session().await?;
    /// # let coll = client.database("stuff").collection::<Document>("stuff");
    /// let mut cursor = coll.find(doc! {}).session(&mut session).await?;
    /// while cursor.advance(&mut session).await? {
    ///     println!("{:?}", cursor.current());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn current(&self) -> &RawDocument {
        self.state.as_ref().unwrap().buffer.current().unwrap()
    }

    /// Deserialize the current result to the generic type associated with this cursor.
    ///
    /// # Panics
    /// [`SessionCursor::advance`] must return `Ok(true)` before
    /// [`SessionCursor::deserialize_current`] can be invoked. Calling
    /// [`SessionCursor::deserialize_current`] after [`SessionCursor::advance`] does not return
    /// true or without calling [`SessionCursor::advance`] at all may result in a panic.
    ///
    /// ```
    /// # use mongodb::{Client, error::Result, bson::doc};
    /// # async fn foo() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
    /// # let mut session = client.start_session().await?;
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
    /// let mut cursor = coll.find(doc! {}).session(&mut session).await?;
    /// while cursor.advance(&mut session).await? {
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
    pub fn with_type<'a, D>(mut self) -> SessionCursor<D>
    where
        D: Deserialize<'a>,
    {
        SessionCursor {
            client: self.client.clone(),
            drop_token: self.drop_token.take(),
            info: self.info.clone(),
            state: Some(self.take_state()),
            drop_address: self.drop_address.take(),
            _phantom: Default::default(),
            #[cfg(test)]
            kill_watcher: self.kill_watcher.take(),
        }
    }

    pub(crate) fn address(&self) -> &ServerAddress {
        &self.info.address
    }

    pub(crate) fn set_drop_address(&mut self, address: ServerAddress) {
        self.drop_address = Some(address);
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
}

impl<T> SessionCursor<T> {
    pub(crate) fn is_exhausted(&self) -> bool {
        self.state.as_ref().is_none_or(|state| state.exhausted)
    }

    #[cfg(test)]
    pub(crate) fn client(&self) -> &Client {
        &self.client
    }
}

impl<T> Drop for SessionCursor<T> {
    fn drop(&mut self) {
        if self.is_exhausted() {
            return;
        }

        kill_cursor(
            self.client.clone(),
            &mut self.drop_token,
            &self.info.ns,
            self.info.id,
            self.state.as_ref().unwrap().pinned_connection.replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            self.kill_watcher.take(),
        );
    }
}

/// A `GenericCursor` that borrows its session.
/// This is to be used with cursors associated with explicit sessions borrowed from the user.
type ExplicitSessionCursor<'session> =
    GenericCursor<'session, ExplicitClientSessionHandle<'session>>;

/// A type that implements [`Stream`](https://docs.rs/futures/latest/futures/stream/index.html) which can be used to
/// stream the results of a [`SessionCursor`]. Returned from [`SessionCursor::stream`].
///
/// This updates the buffer of the parent [`SessionCursor`] when dropped. [`SessionCursor::next`] or
/// any further streams created from [`SessionCursor::stream`] will pick up where this one left off.
pub struct SessionCursorStream<'cursor, 'session, T = Document> {
    session_cursor: &'cursor mut SessionCursor<T>,
    generic_cursor: ExplicitSessionCursor<'session>,
}

impl<T> SessionCursorStream<'_, '_, T>
where
    T: DeserializeOwned,
{
    pub(crate) fn post_batch_resume_token(&self) -> Option<&ResumeToken> {
        self.generic_cursor.post_batch_resume_token()
    }

    pub(crate) fn client(&self) -> &Client {
        &self.session_cursor.client
    }
}

impl<T> Stream for SessionCursorStream<'_, '_, T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        stream_poll_next(&mut self.generic_cursor, cx)
    }
}

impl<T> CursorStream for SessionCursorStream<'_, '_, T>
where
    T: DeserializeOwned,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        self.generic_cursor.poll_next_in_batch(cx)
    }
}

impl<T> Drop for SessionCursorStream<'_, '_, T> {
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.state = Some(self.generic_cursor.take_state());
    }
}
