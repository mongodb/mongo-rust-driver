use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures_core::{future::BoxFuture, Stream};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;

use super::common::{
    kill_cursor,
    CursorInformation,
    GenericCursor,
    GetMoreProvider,
    GetMoreProviderResult,
    PinnedConnection,
};
use crate::{
    bson::Document,
    cmap::conn::PinHandle,
    cursor::CursorSpecification,
    error::{Error, Result},
    operation::GetMore,
    results::GetMoreResult,
    Client,
    ClientSession,
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
    exhausted: bool,
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<T>,
    pinned_connection: PinnedConnection,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(
        client: Client,
        spec: CursorSpecification<T>,
        pinned: Option<PinHandle>,
    ) -> Self {
        let exhausted = spec.id() == 0;

        Self {
            exhausted,
            client,
            info: spec.info,
            buffer: spec.initial_buffer,
            pinned_connection: PinnedConnection::new(pinned),
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
        };
        SessionCursorStream {
            generic_cursor: ExplicitSessionCursor::new(
                self.client.clone(),
                spec,
                self.pinned_connection.clone(),
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
}

impl<T> Drop for SessionCursor<T>
where
    T: DeserializeOwned + Unpin,
{
    fn drop(&mut self) {
        if self.exhausted {
            return;
        }

        kill_cursor(
            self.client.clone(),
            &self.info.ns,
            self.info.id,
            self.pinned_connection.clone(),
        );
    }
}

/// A `GenericCursor` that borrows its session.
/// This is to be used with cursors associated with explicit sessions borrowed from the user.
type ExplicitSessionCursor<'session, T> =
    GenericCursor<ExplicitSessionGetMoreProvider<'session, T>, T>;

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

impl<'cursor, 'session, T> Stream for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.generic_cursor).poll_next(cx)
    }
}

impl<'cursor, 'session, T> Drop for SessionCursorStream<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.buffer = self.generic_cursor.take_buffer();
        self.session_cursor.exhausted = self.generic_cursor.is_exhausted();
    }
}

/// Enum determining whether a `SessionCursorHandle` is excuting a getMore or not.
/// In charge of maintaining ownership of the session reference.
enum ExplicitSessionGetMoreProvider<'session, T> {
    /// The handle is currently executing a getMore via the future.
    ///
    /// This future owns the reference to the session and will return it on completion.
    Executing(BoxFuture<'session, ExecutionResult<'session, T>>),

    /// No future is being executed.
    ///
    /// This variant needs a `MutableSessionReference` struct that can be moved in order to
    /// transition to `Executing` via `take_mut`.
    Idle(MutableSessionReference<'session>),
}

impl<'session, T> ExplicitSessionGetMoreProvider<'session, T> {
    fn new(session: &'session mut ClientSession) -> Self {
        Self::Idle(MutableSessionReference { reference: session })
    }
}

impl<'session, T: Send + Sync + DeserializeOwned> GetMoreProvider
    for ExplicitSessionGetMoreProvider<'session, T>
{
    type DocumentType = T;
    type ResultType = ExecutionResult<'session, T>;
    type GetMoreFuture = BoxFuture<'session, ExecutionResult<'session, T>>;

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
        pinned_connection: Option<&PinHandle>,
    ) {
        let pinned_connection = pinned_connection.cloned();
        take_mut::take(self, |self_| {
            if let ExplicitSessionGetMoreProvider::Idle(session) = self_ {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info);
                    let get_more_result = client
                        .execute_operation_pinned(
                            get_more,
                            Some(&mut *session.reference),
                            pinned_connection.as_ref(),
                        )
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
struct ExecutionResult<'session, T> {
    get_more_result: Result<GetMoreResult<T>>,
    session: &'session mut ClientSession,
}

impl<'session, T> GetMoreProviderResult for ExecutionResult<'session, T> {
    type Session = &'session mut ClientSession;
    type DocumentType = T;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult<T>, &Error> {
        self.get_more_result.as_ref()
    }

    fn into_parts(self) -> (Result<GetMoreResult<T>>, Self::Session) {
        (self.get_more_result, self.session)
    }
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is used to prevent re-borrowing of the session and forcing it to be moved instead
/// by moving the wrapping struct.
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession,
}
