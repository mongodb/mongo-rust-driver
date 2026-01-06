use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use crate::{
    bson::{RawDocument, RawDocumentBuf},
    cmap::RawCommandResponse,
};
use derive_where::derive_where;
use futures_core::{future::BoxFuture, Future};
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    bson::{Bson, Document},
    change_stream::event::ResumeToken,
    client::{session::ClientSession, AsyncDropToken},
    cmap::conn::PinnedConnectionHandle,
    error::{Error, ErrorKind, Result},
    operation::GetMore,
    options::ServerAddress,
    results::GetMoreResult,
    Client,
    Namespace,
};

/// The result of one attempt to advance a cursor.
pub(super) enum AdvanceResult {
    /// The cursor was successfully advanced and the buffer has at least one item.
    Advanced,
    /// The cursor does not have any more items and will not return any more in the future.
    Exhausted,
    /// The cursor does not currently have any items, but future calls to getMore may yield more.
    Waiting,
}

/// An internal cursor that can be used in a variety of contexts depending on its `GetMoreProvider`.
#[derive_where(Debug)]
pub(super) struct GenericCursor<'s, S> {
    #[derive_where(skip)]
    provider: GetMoreProvider<'s, S>,
    client: Client,
    info: CursorInformation,
    /// This is an `Option` to allow it to be "taken" when the cursor is no longer needed
    /// but may be resumed in the future for `SessionCursor`.
    state: Option<CursorState>,
}

impl GenericCursor<'static, ImplicitClientSessionHandle> {
    pub(super) fn with_implicit_session2(
        client: Client,
        spec: CursorSpecification,
        pinned_connection: PinnedConnection,
        session: ImplicitClientSessionHandle,
    ) -> Result<Self> {
        let exhausted = spec.id() == 0;
        Ok(Self {
            client,
            provider: if exhausted {
                GetMoreProvider::Done
            } else {
                GetMoreProvider::Idle(Box::new(session))
            },
            info: spec.info,
            state: Some(CursorState {
                buffer: CursorBuffer::new(reply_batch(&spec.initial_reply)?),
                exhausted,
                post_batch_resume_token: None,
                pinned_connection,
            }),
        })
    }

    /// Extracts the stored implicit [`ClientSession`], if any.
    pub(super) fn take_implicit_session(&mut self) -> Option<ClientSession> {
        self.provider.take_implicit_session()
    }
}

impl<'s> GenericCursor<'s, ExplicitClientSessionHandle<'s>> {
    pub(super) fn with_explicit_session(
        state: CursorState,
        client: Client,
        info: CursorInformation,
        session: ExplicitClientSessionHandle<'s>,
    ) -> Self {
        Self {
            provider: GetMoreProvider::Idle(Box::new(session)),
            client,
            info,
            state: state.into(),
        }
    }
}

impl<'s, S: ClientSessionHandle<'s>> GenericCursor<'s, S> {
    pub(super) fn current(&self) -> Option<&RawDocument> {
        self.state().buffer.current()
    }

    #[cfg(test)]
    pub(super) fn current_batch(&self) -> &VecDeque<RawDocumentBuf> {
        self.state().buffer.as_ref()
    }

    fn state_mut(&mut self) -> &mut CursorState {
        self.state.as_mut().unwrap()
    }

    pub(super) fn state(&self) -> &CursorState {
        self.state.as_ref().unwrap()
    }

    /// Attempt to advance the cursor forward to the next item. If there are no items cached
    /// locally, perform getMores until the cursor is exhausted or the buffer has been refilled.
    /// Return whether or not the cursor has been advanced.
    pub(super) async fn advance(&mut self) -> Result<bool> {
        loop {
            match self.try_advance().await? {
                AdvanceResult::Advanced => return Ok(true),
                AdvanceResult::Exhausted => return Ok(false),
                AdvanceResult::Waiting => continue,
            }
        }
    }

    /// Attempt to advance the cursor forward to the next item. If there are no items cached
    /// locally, perform a single getMore to attempt to retrieve more.
    pub(super) async fn try_advance(&mut self) -> Result<AdvanceResult> {
        if self.state_mut().buffer.advance() {
            return Ok(AdvanceResult::Advanced);
        } else if self.is_exhausted() {
            return Ok(AdvanceResult::Exhausted);
        }

        // If the buffer is empty but the cursor is not exhausted, perform a getMore.
        let client = self.client.clone();
        let spec = self.info.clone();
        let pin = self.state().pinned_connection.replicate();

        let result = self.provider.execute(spec, client, pin).await;
        self.handle_get_more_result(result)?;

        match self.state_mut().buffer.advance() {
            true => Ok(AdvanceResult::Advanced),
            false => {
                if self.is_exhausted() {
                    Ok(AdvanceResult::Exhausted)
                } else {
                    Ok(AdvanceResult::Waiting)
                }
            }
        }
    }

    pub(super) fn take_state(&mut self) -> CursorState {
        self.state.take().unwrap()
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.state().exhausted
    }

    pub(super) fn id(&self) -> i64 {
        self.info.id
    }

    pub(super) fn namespace(&self) -> &Namespace {
        &self.info.ns
    }

    pub(super) fn address(&self) -> &ServerAddress {
        &self.info.address
    }

    pub(super) fn pinned_connection(&self) -> &PinnedConnection {
        &self.state().pinned_connection
    }

    pub(super) fn post_batch_resume_token(&self) -> Option<&ResumeToken> {
        self.state().post_batch_resume_token.as_ref()
    }

    fn mark_exhausted(&mut self) {
        self.state_mut().exhausted = true;
        self.state_mut().pinned_connection = PinnedConnection::Unpinned;
    }

    fn handle_get_more_result(&mut self, get_more_result: Result<GetMoreResult>) -> Result<()> {
        match get_more_result {
            Ok(get_more) => {
                if get_more.exhausted {
                    self.mark_exhausted();
                }
                if get_more.id != 0 {
                    self.info.id = get_more.id
                }
                self.info.ns = get_more.ns;
                self.state_mut().buffer = CursorBuffer::new(reply_batch(&get_more.raw_reply)?);
                self.state_mut().post_batch_resume_token = get_more.post_batch_resume_token;

                Ok(())
            }
            Err(e) => {
                if matches!(*e.kind, ErrorKind::Command(ref e) if e.code == 43 || e.code == 237) {
                    self.mark_exhausted();
                }

                if e.is_network_error() {
                    // Flag the connection as invalid, preventing a killCursors command,
                    // but leave the connection pinned.
                    self.state_mut().pinned_connection.invalidate();
                }

                Err(e)
            }
        }
    }
}

pub(crate) trait CursorStream {
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>>;
}

pub(crate) enum BatchValue {
    Some { doc: RawDocumentBuf, is_last: bool },
    Empty,
    Exhausted,
}

impl<'s, S: ClientSessionHandle<'s>> CursorStream for GenericCursor<'s, S> {
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        // If there is a get more in flight, check on its status.
        if let Some(future) = self.provider.executing_future() {
            match Pin::new(future).poll(cx) {
                // If a result is ready, retrieve the buffer and update the exhausted status.
                Poll::Ready(get_more_result_and_session) => {
                    let output = self.handle_get_more_result(get_more_result_and_session.result);
                    self.provider.clear_execution(
                        get_more_result_and_session.session,
                        self.state().exhausted,
                    );
                    output?;
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        match self.state_mut().buffer.next() {
            Some(doc) => {
                let is_last = self.state().buffer.is_empty();

                Poll::Ready(Ok(BatchValue::Some { doc, is_last }))
            }
            None if !self.state().exhausted && !self.state().pinned_connection.is_invalid() => {
                let info = self.info.clone();
                let client = self.client.clone();
                let state = self.state.as_mut().unwrap();
                self.provider
                    .start_execution(info, client, state.pinned_connection.handle());
                Poll::Ready(Ok(BatchValue::Empty))
            }
            None => Poll::Ready(Ok(BatchValue::Exhausted)),
        }
    }
}

// To avoid a private trait (`CursorStream`) in a public interface (`impl Stream`), this is provided
// as a free function rather than a blanket impl.
pub(crate) fn stream_poll_next<S, V>(this: &mut S, cx: &mut Context<'_>) -> Poll<Option<Result<V>>>
where
    S: CursorStream,
    V: for<'a> serde::Deserialize<'a>,
{
    loop {
        match this.poll_next_in_batch(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(bv) => match bv? {
                BatchValue::Some { doc, .. } => {
                    return Poll::Ready(Some(Ok(crate::bson_compat::deserialize_from_slice(
                        doc.as_bytes(),
                    )?)))
                }
                BatchValue::Empty => continue,
                BatchValue::Exhausted => return Poll::Ready(None),
            },
        }
    }
}

pub(crate) struct NextInBatchFuture<'a, T>(&'a mut T);

impl<'a, T> NextInBatchFuture<'a, T>
where
    T: CursorStream,
{
    pub(crate) fn new(stream: &'a mut T) -> Self {
        Self(stream)
    }
}

impl<C> Future for NextInBatchFuture<'_, C>
where
    C: CursorStream,
{
    type Output = Result<BatchValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_next_in_batch(cx)
    }
}

/// Provides batches of documents to a cursor via the `getMore` command.
enum GetMoreProvider<'s, S> {
    Executing(BoxFuture<'s, GetMoreResultAndSession<S>>),
    // `Box` is used to make the size of `Idle` similar to that of the other variants.
    Idle(Box<S>),
    Done,
}

impl GetMoreProvider<'static, ImplicitClientSessionHandle> {
    /// Extracts the stored implicit [`ClientSession`], if any.
    /// The provider cannot be started again after this call.
    fn take_implicit_session(&mut self) -> Option<ClientSession> {
        match self {
            Self::Idle(session) => session.take_implicit_session(),
            Self::Executing(..) | Self::Done => None,
        }
    }
}

impl<'s, S: ClientSessionHandle<'s>> GetMoreProvider<'s, S> {
    /// Get the future being evaluated, if there is one.
    fn executing_future(&mut self) -> Option<&mut BoxFuture<'s, GetMoreResultAndSession<S>>> {
        if let Self::Executing(future) = self {
            Some(future)
        } else {
            None
        }
    }

    /// Clear out any state remaining from previous `getMore` executions.
    fn clear_execution(&mut self, session: S, exhausted: bool) {
        if exhausted && session.is_implicit() {
            *self = Self::Done
        } else {
            *self = Self::Idle(Box::new(session))
        }
    }

    /// Start executing a new `getMore` if one is not already in flight.
    fn start_execution(
        &mut self,
        info: CursorInformation,
        client: Client,
        pinned_connection: Option<&PinnedConnectionHandle>,
    ) {
        take_mut::take(self, |self_| {
            if let Self::Idle(mut session) = self_ {
                let pinned_connection = pinned_connection.map(|c| c.replicate());
                let future = Box::pin(async move {
                    let get_more = GetMore::new(info, pinned_connection.as_ref());
                    let get_more_result = client
                        .execute_operation(get_more, session.borrow_mut())
                        .await;
                    GetMoreResultAndSession {
                        result: get_more_result,
                        session: *session,
                    }
                });
                Self::Executing(future)
            } else {
                self_
            }
        })
    }

    /// Return a future that will execute the `getMore` when polled.
    /// This is useful in `async` functions that can `.await` the entire `getMore` process.
    /// [`GetMoreProvider::start_execution`] and [`GetMoreProvider::clear_execution`]
    /// should be used for contexts where the futures need to be [`poll`](Future::poll)ed manually.
    fn execute(
        &mut self,
        info: CursorInformation,
        client: Client,
        pinned_connection: PinnedConnection,
    ) -> BoxFuture<'_, Result<GetMoreResult>> {
        match self {
            Self::Idle(ref mut session) => Box::pin(async move {
                let get_more = GetMore::new(info, pinned_connection.handle());
                client
                    .execute_operation(get_more, session.borrow_mut())
                    .await
            }),
            Self::Executing(_fut) => Box::pin(async {
                Err(Error::internal(
                    "streaming the cursor was cancelled while a request was in progress and must \
                     be continued before iterating manually",
                ))
            }),
            Self::Done => {
                // this should never happen
                Box::pin(async { Err(Error::internal("cursor iterated after already exhausted")) })
            }
        }
    }
}

struct GetMoreResultAndSession<S> {
    result: Result<GetMoreResult>,
    session: S,
}

/// Specification used to create a new cursor.
#[derive(Debug, Clone)]
pub(crate) struct CursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_reply: RawDocumentBuf,
    pub(crate) is_empty: bool,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
}

impl CursorSpecification {
    pub(crate) fn new(
        response: RawCommandResponse,
        address: ServerAddress,
        batch_size: impl Into<Option<u32>>,
        max_time: impl Into<Option<Duration>>,
        comment: impl Into<Option<Bson>>,
    ) -> Result<Self> {
        // Parse minimal fields via raw to avoid per-doc copies.
        let raw_root = response.raw_body();
        let cursor_doc = raw_root.get_document("cursor")?;
        let id = cursor_doc.get_i64("id")?;
        let ns_str = cursor_doc.get_str("ns")?;
        let ns = Namespace::from_str(ns_str)
            .ok_or_else(|| Error::invalid_response("invalid cursor ns"))?;
        let post_token_raw = cursor_doc
            .get("postBatchResumeToken")?
            .and_then(crate::bson::RawBsonRef::as_document)
            .map(|d| d.to_owned());
        let first_batch = cursor_doc.get_array("firstBatch")?;
        let is_empty = first_batch.is_empty();
        let post_batch_resume_token =
            crate::change_stream::event::ResumeToken::from_raw(post_token_raw);
        Ok(Self {
            info: CursorInformation {
                ns,
                id,
                address,
                batch_size: batch_size.into(),
                max_time: max_time.into(),
                comment: comment.into(),
            },
            initial_reply: response.into_raw_document_buf(),
            is_empty,
            post_batch_resume_token,
        })
    }

    pub(crate) fn id(&self) -> i64 {
        self.info.id
    }
}

/// Static information about a cursor.
#[derive(Clone, Debug)]
pub(crate) struct CursorInformation {
    pub(crate) ns: Namespace,
    pub(crate) address: ServerAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
    pub(crate) comment: Option<Bson>,
}

#[derive(Debug)]
pub(crate) enum PinnedConnection {
    Valid(PinnedConnectionHandle),
    Invalid(PinnedConnectionHandle),
    Unpinned,
}

impl PinnedConnection {
    pub(super) fn new(handle: Option<PinnedConnectionHandle>) -> Self {
        match handle {
            Some(h) => Self::Valid(h),
            None => Self::Unpinned,
        }
    }

    /// Make a new `PinnedConnection` that refers to the same connection as this one.
    /// Use with care and only when "lending" a handle in a way that can't be expressed as a
    /// normal borrow.
    pub(crate) fn replicate(&self) -> Self {
        match self {
            Self::Valid(h) => Self::Valid(h.replicate()),
            Self::Invalid(h) => Self::Invalid(h.replicate()),
            Self::Unpinned => Self::Unpinned,
        }
    }

    pub(crate) fn handle(&self) -> Option<&PinnedConnectionHandle> {
        match self {
            Self::Valid(h) | Self::Invalid(h) => Some(h),
            Self::Unpinned => None,
        }
    }

    fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }

    fn invalidate(&mut self) {
        take_mut::take(self, |self_| {
            if let Self::Valid(c) = self_ {
                Self::Invalid(c)
            } else {
                self_
            }
        });
    }
}

pub(super) fn kill_cursor(
    client: Client,
    drop_token: &mut AsyncDropToken,
    ns: &Namespace,
    cursor_id: i64,
    pinned_conn: PinnedConnection,
    drop_address: Option<ServerAddress>,
    #[cfg(test)] kill_watcher: Option<oneshot::Sender<()>>,
) {
    let coll = client
        .database(ns.db.as_str())
        .collection::<Document>(ns.coll.as_str());
    drop_token.spawn(async move {
        if !pinned_conn.is_invalid() {
            let _ = coll
                .kill_cursor(cursor_id, pinned_conn.handle(), drop_address)
                .await;
            #[cfg(test)]
            if let Some(tx) = kill_watcher {
                let _ = tx.send(());
            }
        }
    });
}

#[derive(Debug)]
pub(crate) struct CursorState {
    pub(crate) buffer: CursorBuffer,
    pub(crate) exhausted: bool,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
    pub(crate) pinned_connection: PinnedConnection,
}

#[derive(Debug, Clone)]
pub(crate) struct CursorBuffer {
    docs: VecDeque<RawDocumentBuf>,
    /// whether the buffer is at the front or not
    fresh: bool,
}

impl CursorBuffer {
    pub(crate) fn new(initial_buffer: VecDeque<RawDocumentBuf>) -> Self {
        Self {
            docs: initial_buffer,
            fresh: true,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Removes and returns the document in the front of the buffer.
    pub(crate) fn next(&mut self) -> Option<RawDocumentBuf> {
        self.fresh = false;
        self.docs.pop_front()
    }

    /// Advances the buffer to the next document. Returns whether there are any documents remaining
    /// in the buffer after advancing.
    pub(crate) fn advance(&mut self) -> bool {
        // If at the front of the buffer, don't move forward as the first document hasn't been
        // consumed yet.
        if self.fresh {
            self.fresh = false;
        } else {
            self.docs.pop_front();
        }
        !self.is_empty()
    }

    /// Returns the item at the front of the buffer, if there is one. This method does not change
    /// the state of the buffer.
    pub(crate) fn current(&self) -> Option<&RawDocument> {
        self.docs.front().map(|d| d.as_ref())
    }
}

impl AsRef<VecDeque<RawDocumentBuf>> for CursorBuffer {
    fn as_ref(&self) -> &VecDeque<RawDocumentBuf> {
        &self.docs
    }
}

#[test]
fn test_buffer() {
    use crate::bson::rawdoc;

    let queue: VecDeque<RawDocumentBuf> =
        [rawdoc! { "x": 1 }, rawdoc! { "x": 2 }, rawdoc! { "x": 3 }].into();
    let mut buffer = CursorBuffer::new(queue);

    assert!(buffer.advance());
    assert_eq!(buffer.current(), Some(rawdoc! { "x": 1 }.as_ref()));

    assert!(buffer.advance());
    assert_eq!(buffer.current(), Some(rawdoc! { "x": 2 }.as_ref()));

    assert!(buffer.advance());
    assert_eq!(buffer.current(), Some(rawdoc! { "x": 3 }.as_ref()));

    assert!(!buffer.advance());
    assert_eq!(buffer.current(), None);
}

pub(super) struct ImplicitClientSessionHandle(pub(super) Option<ClientSession>);

impl ImplicitClientSessionHandle {
    fn take_implicit_session(&mut self) -> Option<ClientSession> {
        self.0.take()
    }
}

impl ClientSessionHandle<'_> for ImplicitClientSessionHandle {
    fn is_implicit(&self) -> bool {
        true
    }

    fn borrow_mut(&mut self) -> Option<&mut ClientSession> {
        self.0.as_mut()
    }
}

pub(super) struct ExplicitClientSessionHandle<'a>(pub(super) &'a mut ClientSession);

impl<'a> ClientSessionHandle<'a> for ExplicitClientSessionHandle<'a> {
    fn is_implicit(&self) -> bool {
        false
    }

    fn borrow_mut(&mut self) -> Option<&mut ClientSession> {
        Some(self.0)
    }
}

pub(super) trait ClientSessionHandle<'a>: Send + 'a {
    fn is_implicit(&self) -> bool;

    fn borrow_mut(&mut self) -> Option<&mut ClientSession>;
}

pub(crate) fn reply_batch(
    reply: &RawDocument,
) -> Result<VecDeque<crate::bson::raw::RawDocumentBuf>> {
    let cursor = reply.get_document("cursor")?;
    let docs = match cursor.get("firstBatch")? {
        Some(d) => d
            .as_array()
            .ok_or_else(|| Error::invalid_response("invalid `firstBatch` value"))?,
        None => cursor.get_array("nextBatch")?,
    };
    let mut out = VecDeque::new();
    for elt in docs {
        let elt = elt?;
        let doc = match elt.as_document() {
            Some(doc) => doc.to_owned(),
            None => {
                return Err(crate::error::Error::invalid_response(
                    "invalid batch element",
                ))
            }
        };
        out.push_back(doc);
    }
    Ok(out)
}
