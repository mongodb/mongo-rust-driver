use std::{
    collections::VecDeque,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::{future::BoxFuture, Stream};
use futures_util::StreamExt;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use super::common::{CursorInformation, GenericCursor, GetMoreProvider, GetMoreProviderResult};
use crate::{
    bson::Document,
    cmap::conn::Connection,
    cursor::{CursorSpecification, GetMoreContext},
    error::{Error, Result},
    operation::GetMore,
    results::GetMoreResult,
    Client,
    ClientSession,
    RUNTIME,
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
    pinned_connection: Option<Arc<Mutex<Connection>>>,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin + Send + Sync,
{
    pub(crate) fn new(client: Client, spec: CursorSpecification<T>, pinned_connection: Option<Connection>) -> Self {
        let exhausted = spec.id() == 0;

        Self {
            exhausted,
            client,
            info: spec.info,
            buffer: spec.initial_buffer,
            pinned_connection: pinned_connection.map(|c| Arc::new(Mutex::new(c))),
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
        let get_more_provider = ExplicitSessionGetMoreProvider::new(session, self.pinned_connection.clone());

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

        let ns = &self.info.ns;
        let coll = self
            .client
            .database(ns.db.as_str())
            .collection::<Document>(ns.coll.as_str());
        let cursor_id = self.info.id;
        RUNTIME.execute(async move { coll.kill_cursor(cursor_id).await });
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
    Idle {
        session: MutableSessionReference<'session>,
        pinned_connection: Option<Arc<Mutex<Connection>>>,
    },
}

impl<'session, T> ExplicitSessionGetMoreProvider<'session, T> {
    fn new(session: &'session mut ClientSession, pinned_connection: Option<Arc<Mutex<Connection>>>) -> Self {
        Self::Idle {
            session: MutableSessionReference { reference: session },
            pinned_connection,
        }
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

    fn clear_execution(&mut self, context: GetMoreContext<Self::ResultType>, _exhausted: bool) {
        *self = Self::Idle {
            session: MutableSessionReference { reference: context.session },
            pinned_connection: context.pinned_connection,
        }
    }

    fn start_execution(&mut self, info: CursorInformation, client: Client) {
        take_mut::take(self, |self_| {
            if let ExplicitSessionGetMoreProvider::Idle { session, pinned_connection } = self_ {
                let future = Box::pin(async move {
                    let mut conn_lock = match pinned_connection {
                        Some(ref c) => Some(c.lock().await),
                        None => None,
                    };
                    let conn = conn_lock.as_mut().map(|l| &mut **l);
                    let get_more = GetMore::new(info);
                    let get_more_result = client
                        .execute_operation_pinned(get_more, Some(&mut *session.reference), conn)
                        .await;
                    drop(conn_lock);
                    ExecutionResult {
                        get_more_result,
                        context: GetMoreContext::new(session.reference, pinned_connection),
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
    context: GetMoreContext<Self>,
}

impl<'session, T> GetMoreProviderResult for ExecutionResult<'session, T> {
    type Session = &'session mut ClientSession;
    type PinnedConnection = Arc<Mutex<Connection>>;
    type DocumentType = T;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult<T>, &Error> {
        self.get_more_result.as_ref()
    }

    fn into_parts(self) -> (Result<GetMoreResult<T>>, GetMoreContext<Self>) {
        (self.get_more_result, self.context)
    }
}

/// Wrapper around a mutable reference to a `ClientSession` that provides move semantics.
/// This is used to prevent re-borrowing of the session and forcing it to be moved instead
/// by moving the wrapping struct.
struct MutableSessionReference<'a> {
    reference: &'a mut ClientSession,
}
