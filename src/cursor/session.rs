use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{future::BoxFuture, Stream};
use serde::de::DeserializeOwned;

use super::common::{CursorInformation, GenericCursor, GetMoreProvider, GetMoreProviderResult};
use crate::{
    bson::{from_document, Document},
    cursor::CursorSpecification,
    error::{Error, Result},
    operation::GetMore,
    results::GetMoreResult,
    Client,
    ClientSession,
    RUNTIME,
};

/// A `SessionCursor` is a cursor that was created with a `ClientSession` and must be iterated using
/// one. To iterate, retrieve a `SessionCursorHandle` using `SessionCursor::with_session`:
///
/// ```rust
/// # use futures::stream::StreamExt;
/// # use mongodb::{bson::Document, Client, error::Result, ClientSession, SessionCursor};
/// #
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let mut session = client.start_session(None).await?;
/// # let coll = client.database("foo").collection::<Document>("bar");
/// # let mut cursor = coll.find_with_session(None, None, &mut session).await?;
/// #
/// while let Some(doc) = cursor.with_session(&mut session).next().await {
///     println!("{}", doc?)
/// }
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
    buffer: VecDeque<Document>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> SessionCursor<T>
where
    T: DeserializeOwned + Unpin,
{
    pub(crate) fn new(client: Client, spec: CursorSpecification) -> Self {
        let exhausted = spec.id() == 0;

        Self {
            exhausted,
            client,
            info: spec.info,
            buffer: spec.initial_buffer,
            _phantom: Default::default(),
        }
    }

    /// Retrieves a `SessionCursorHandle` to iterate this cursor. The session provided must be the
    /// same session used to create the cursor.
    pub fn with_session<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionCursorHandle<'_, 'session, T> {
        let get_more_provider = ExplicitSessionGetMoreProvider::new(session);

        // Pass the buffer into this cursor handle for iteration.
        // It will be returned in the handle's `Drop` implementation.
        let spec = CursorSpecification {
            info: self.info.clone(),
            initial_buffer: std::mem::take(&mut self.buffer),
        };
        SessionCursorHandle {
            generic_cursor: ExplicitSessionCursor::new(
                self.client.clone(),
                spec,
                get_more_provider,
            ),
            session_cursor: self,
        }
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
type ExplicitSessionCursor<'session> = GenericCursor<ExplicitSessionGetMoreProvider<'session>>;

/// A handle that borrows a `ClientSession` temporarily for executing getMores or iterating through
/// the current buffer of a `SessionCursor`.
///
/// This updates the buffer of the parent `SessionCursor` when dropped.
pub struct SessionCursorHandle<'cursor, 'session, T = Document>
where
    T: DeserializeOwned + Unpin,
{
    session_cursor: &'cursor mut SessionCursor<T>,
    generic_cursor: ExplicitSessionCursor<'session>,
}

impl<'cursor, 'session, T> Stream for SessionCursorHandle<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = Pin::new(&mut self.generic_cursor).poll_next(cx);
        match next {
            Poll::Ready(opt) => Poll::Ready(
                opt.map(|result| result.and_then(|doc| from_document(doc).map_err(Into::into))),
            ),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<'cursor, 'session, T> Drop for SessionCursorHandle<'cursor, 'session, T>
where
    T: DeserializeOwned + Unpin,
{
    fn drop(&mut self) {
        // Update the parent cursor's state based on any iteration performed on this handle.
        self.session_cursor.buffer = self.generic_cursor.take_buffer();
        self.session_cursor.exhausted = self.generic_cursor.is_exhausted();
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
    type GetMoreResult = ExecutionResult<'session>;
    type GetMoreFuture = BoxFuture<'session, ExecutionResult<'session>>;

    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture> {
        match self {
            Self::Executing(future) => Some(future),
            Self::Idle(_) => None,
        }
    }

    fn clear_execution(&mut self, session: &'session mut ClientSession, _exhausted: bool) {
        *self = Self::Idle(MutableSessionReference { reference: session })
    }

    fn start_execution(&mut self, info: CursorInformation, client: Client) {
        take_mut::take(self, |self_| {
            if let ExplicitSessionGetMoreProvider::Idle(session) = self_ {
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info);
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
