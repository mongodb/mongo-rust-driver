use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bson::RawDocumentBuf;
use futures_core::{future::BoxFuture, Stream};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
#[cfg(test)]
use tokio::sync::oneshot;

use super::{
    common::{
        kill_cursor,
        CursorInformation,
        GenericCursor,
        GetMoreProvider,
        GetMoreProviderResult,
        PinnedConnection,
    },
    BatchValue,
    CursorStream,
};
use crate::{
    bson::Document,
    change_stream::event::ResumeToken,
    cmap::conn::PinnedConnectionHandle,
    cursor::CursorSpecification,
    error::{Error, Result},
    operation::GetMore,
    results::GetMoreResult,
    Client,
    ClientSession, client::options::ServerAddress,
};

/// A [`SessionCursor`] is a cursor that was created with a [`ClientSession`] that must be iterated
/// using one. To iterate, use [`SessionCursor::next`] or retrieve a [`SessionCursorStream`] using
/// [`SessionCursor::stream`]:
///
/// ```rust
/// # use mongodb::{bson::Document, Client, error::Result, ClientSession, SessionCursor};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let mut session = client.start_session(None).await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// #
/// // iterate using next()
/// let mut cursor = coll.find_with_session(None, None, &mut session).await?;
/// while let Some(doc) = cursor.next(&mut session).await.transpose()? {
///     println!("{}", doc)
/// }
///
/// // iterate using `Stream`:
/// use futures::stream::TryStreamExt;
///
/// let mut cursor = coll.find_with_session(None, None, &mut session).await?;
/// let results: Vec<_> = cursor.stream(&mut session).try_collect().await?;
/// #
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct SessionCursor<T>
where
    T: DeserializeOwned + Unpin,
{
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<RawDocumentBuf>,
    pinned_connection: PinnedConnection,
    drop_address: Option<ServerAddress>,
    _phantom: PhantomData<T>,
    #[cfg(test)]
    kill_watcher: Option<oneshot::Sender<()>>,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification,
        pinned: Option<PinnedConnectionHandle>,
    ) -> Self {
        Self {
            client,
            info: spec.info,
            buffer: spec.initial_buffer,
            pinned_connection: PinnedConnection::new(pinned),
            drop_address: None,
            _phantom: Default::default(),
            #[cfg(test)]
            kill_watcher: None,
        }
    }

    /// Retrieves a [`SessionCursorStream`] to iterate this cursor. The session provided must be the
    /// same session used to create the cursor.
    ///
    /// Note that the borrow checker will not allow the session to be reused in between iterations
    /// of this stream. In order to do that, either use [`SessionCursor::next`] instead or drop
    /// the stream before using the session.
    ///
    /// ```
    /// # use bson::{doc, Document};
    /// # use mongodb::{Client, error::Result};
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session(None).await?;
    /// #
    /// use futures::stream::TryStreamExt;
    ///
    /// // iterate over the results
    /// let mut cursor = coll.find_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// while let Some(doc) = cursor.stream(&mut session).try_next().await? {
    ///     println!("{}", doc);
    /// }
    ///
    /// // collect the results
    /// let mut cursor1 = coll.find_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// let v: Vec<Document> = cursor1.stream(&mut session).try_collect().await?;
    ///
    /// // use session between iterations
    /// let mut cursor2 = coll.find_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// loop {
    ///     let doc = match cursor2.stream(&mut session).try_next().await? {
    ///         Some(d) => d,
    ///         None => break,
    ///     };
    ///     other_coll.insert_one_with_session(doc, None, &mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub fn stream<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorStream<'_, 'session, T> {
        let get_more_provider = ExplicitSessionGetMoreProvider::new(session);

        // Pass the buffer into this cursor handle for iteration.
        // It will be returned in the handle's `Drop` implementation.
        let spec = CursorSpecification {
            info: self.info.clone(),
            initial_buffer: std::mem::take(&mut self.buffer),
            post_batch_resume_token: None,
        };
        SessionCursorStream {
            generic_cursor: ExplicitSessionCursor::new(
                self.client.clone(),
                spec,
                self.pinned_connection.replicate(),
                get_more_provider,
            ),
            session_cursor: self,
        }
    }

    /// Retrieve the next result from the cursor.
    /// The session provided must be the same session used to create the cursor.
    ///
    /// Use this method when the session needs to be used again between iterations or when the added
    /// functionality of `Stream` is not needed.
    ///
    /// ```
    /// # use bson::{doc, Document};
    /// # use mongodb::Client;
    /// # fn main() {
    /// # async {
    /// # let client = Client::with_uri_str("foo").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let other_coll = coll.clone();
    /// # let mut session = client.start_session(None).await?;
    /// let mut cursor = coll.find_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// while let Some(doc) = cursor.next(&mut session).await.transpose()? {
    ///     other_coll.insert_one_with_session(doc, None, &mut session).await?;
    /// }
    /// # Ok::<(), mongodb::error::Error>(())
    /// # };
    /// # }
    /// ```
    pub async fn next(&mut self, session: &mut ClientSession) -> Option<Result<T>> {
        self.stream(session).next().await
    }

    /// Update the type streamed values will be parsed as.
    pub fn with_type<D>(mut self) -> SessionCursor<D>
    where
        D: DeserializeOwned + Unpin + Send + Sync,
    {
        let out = SessionCursor {
            client: self.client.clone(),
            info: self.info.clone(),
            buffer: std::mem::take(&mut self.buffer),
            pinned_connection: self.pinned_connection.take(),
            drop_address: self.drop_address.take(),
            _phantom: Default::default(),
            #[cfg(test)]
            kill_watcher: self.kill_watcher.take(),
        };
        self.mark_exhausted(); // prevent a `kill_cursor` call in `drop`
        out
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

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin,
{
    fn mark_exhausted(&mut self) {
        self.info.id = 0;
    }

    pub(crate) fn is_exhausted(&self) -> bool {
        self.info.id == 0
    }
}

impl<T> Drop for SessionCursor<T>
where
    T: DeserializeOwned + Unpin,
{
    fn drop(&mut self) {
        if self.is_exhausted() {
            return;
        }

        kill_cursor(
            self.client.clone(),
            &self.info.ns,
            self.info.id,
            self.pinned_connection.replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            self.kill_watcher.take(),
        );
    }
}

/// A `GenericCursor` that borrows its session.
/// This is to be used with cursors associated with explicit sessions borrowed from the user.
type ExplicitSessionCursor<'session, T> =
    GenericCursor<ExplicitSessionGetMoreProvider<'session>, T>;

/// A type that implements [`Stream`](https://docs.rs/futures/latest/futures/stream/index.html) which can be used to
/// stream the results of a [`SessionCursor`]. Returned from [`SessionCursor::stream`].
///
/// This updates the buffer of the parent [`SessionCursor`] when dropped. [`SessionCursor::next`] or
/// any further streams created from [`SessionCursor::stream`] will pick up where this one left off.
pub struct SessionCursorStream<'cursor, 'session, T = Document>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    session_cursor: &'cursor mut SessionCursor<T>,
    generic_cursor: ExplicitSessionCursor<'session, T>,
}

impl<'cursor, 'session, T> SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn post_batch_resume_token(&self) -> Option<&ResumeToken> {
        self.generic_cursor.post_batch_resume_token()
    }

    pub(crate) fn client(&self) -> &Client {
        &self.session_cursor.client
    }
}

impl<'cursor, 'session, T> Stream for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.generic_cursor).poll_next(cx)
    }
}

impl<'cursor, 'session, T> CursorStream for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        self.generic_cursor.poll_next_in_batch(cx)
    }
}

impl<'cursor, 'session, T> Drop for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.buffer = self.generic_cursor.take_buffer();
        if self.generic_cursor.is_exhausted() {
            self.session_cursor.mark_exhausted();
        }
    }
}

/// Enum determining whether a `SessionCursorHandle` is excuting a getMore or not.
/// In charge of maintaining ownership of the session reference.
enum ExplicitSessionGetMoreProvider<'session> {
    /// The handle is currently executing a getMore via the future.
    ///
    /// This future owns the reference to the session and will return it on completion.
    Executing(BoxFuture<'session, ExecutionResult<'session>>),

    /// No future is being executed.
    ///
    /// This variant needs a `MutableSessionReference` struct that can be moved in order to
    /// transition to `Executing` via `take_mut`.
    Idle(MutableSessionReference<'session>),
}

impl<'session> ExplicitSessionGetMoreProvider<'session> {
    fn new(session: &'session mut ClientSession) -> Self {
        Self::Idle(MutableSessionReference { reference: session })
    }
}

impl<'session> GetMoreProvider for ExplicitSessionGetMoreProvider<'session> {
    type ResultType = ExecutionResult<'session>;
    type GetMoreFuture = BoxFuture<'session, ExecutionResult<'session>>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(future) => Some(future),
            Self::Idle { .. } => None,
        }
    }

    fn clear_execution(&mut self, session: &'session mut ClientSession, _exhausted: bool) {
        *self = Self::Idle(MutableSessionReference { reference: session })
    }

    fn start_execution(
        &mut self,
        info: CursorInformation,
        client: Client,
        pinned_connection: Option<&PinnedConnectionHandle>,
    ) {
        take_mut::take(self, |self_| {
            if let ExplicitSessionGetMoreProvider::Idle(session) = self_ {
                let pinned_connection = pinned_connection.map(|c| c.replicate());
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info, pinned_connection.as_ref());
                    let get_more_result = client
                        .execute_operation(get_more, Some(&mut *session.reference))
                        .await;
                    ExecutionResult {
                        get_more_result,
                        session: session.reference,
                    }
                });
                return ExplicitSessionGetMoreProvider::Executing(future);
            }
            self_
        });
    }
}

/// Struct returned from awaiting on a `GetMoreFuture` containing the result of the getMore as
/// well as the reference to the `ClientSession` used for the getMore.
struct ExecutionResult<'session> {
    get_more_result: Result<GetMoreResult>,
    session: &'session mut ClientSession,
}

impl<'session> GetMoreProviderResult for ExecutionResult<'session> {
    type Session = &'session mut ClientSession;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult, &Error> {
        self.get_more_result.as_ref()
    }

    fn into_parts(self) -> (Result<GetMoreResult>, Self::Session) {
        (self.get_more_result, self.session)
    }
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is used to prevent re-borrowing of the session and forcing it to be moved instead
/// by moving the wrapping struct.
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession,
}
