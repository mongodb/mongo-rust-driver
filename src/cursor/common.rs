use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bson::{RawDocument, RawDocumentBuf};
use derivative::Derivative;
use futures_core::{future::BoxFuture, Future, Stream};
use serde::{de::DeserializeOwned, Deserialize};
#[cfg(test)]
use tokio::sync::oneshot;

use crate::{
    bson::Document,
    change_stream::event::ResumeToken,
    cmap::conn::PinnedConnectionHandle,
    error::{Error, ErrorKind, Result},
    operation,
    options::ServerAddress,
    results::GetMoreResult,
    Client,
    Namespace,
    RUNTIME,
};

/// An internal cursor that can be used in a variety of contexts depending on its `GetMoreProvider`.
#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct GenericCursor<P, T>
where
    P: GetMoreProvider,
{
    #[derivative(Debug = "ignore")]
    provider: P,
    client: Client,
    info: CursorInformation,
    /// This is an `Option` to allow it to be "taken" when the cursor is no longer needed
    /// but may be resumed in the future for `SessionCursor`.
    state: Option<CursorState>,
    _phantom: PhantomData<T>,
}

impl<P, T> GenericCursor<P, T>
where
    P: GetMoreProvider,
{
    pub(super) fn new(
        client: Client,
        spec: CursorSpecification,
        pinned_connection: PinnedConnection,
        get_more_provider: P,
    ) -> Self {
        let exhausted = spec.id() == 0;
        Self {
            client,
            provider: get_more_provider,
            info: spec.info,
            state: Some(CursorState {
                buffer: CursorBuffer::new(spec.initial_buffer),
                error: None,
                exhausted,
                post_batch_resume_token: None,
                pinned_connection,
            }),
            _phantom: Default::default(),
        }
    }

    pub(super) fn from_state(
        state: CursorState,
        client: Client,
        info: CursorInformation,
        provider: P,
    ) -> Self {
        Self {
            provider,
            client,
            info,
            _phantom: Default::default(),
            state: state.into(),
        }
    }

    pub(super) fn current(&self) -> Option<&RawDocument> {
        self.state.as_ref().unwrap().buffer.current()
    }

    fn state_mut(&mut self) -> &mut CursorState {
        self.state.as_mut().unwrap()
    }

    fn state(&self) -> &CursorState {
        self.state.as_ref().unwrap()
    }

    /// Advance the cursor forward to the next document.
    /// If there are no documents cached locally, perform getMores until
    /// the cursor is exhausted or a result/error has been received.
    pub(super) async fn advance(&mut self) -> Result<bool> {
        loop {
            self.state_mut().buffer.advance();

            if !self.state().buffer.is_empty() {
                break;
            }

            // if moving the offset puts us at the end of the buffer, perform another
            // getMore if the cursor is still alive.

            if self.state().exhausted {
                return Ok(false);
            }

            let client = self.client.clone();
            let spec = self.info.clone();
            let pin = self.state().pinned_connection.replicate();

            let result = self.provider.execute(spec, client, pin).await;
            self.handle_get_more_result(result)?;
        }

        Ok(true)
    }

    pub(super) fn take_state(&mut self) -> CursorState {
        self.state.take().unwrap()
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.state.as_ref().unwrap().exhausted
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
        &self.state.as_ref().unwrap().pinned_connection
    }

    pub(super) fn post_batch_resume_token(&self) -> Option<&ResumeToken> {
        self.state
            .as_ref()
            .unwrap()
            .post_batch_resume_token
            .as_ref()
    }

    fn handle_get_more_result(&mut self, get_more_result: Result<GetMoreResult>) -> Result<()> {
        self.state_mut().exhausted = get_more_result
            .as_ref()
            .map(|r| r.exhausted)
            .unwrap_or(true);
        if self.state().exhausted {
            self.state_mut().pinned_connection = PinnedConnection::Unpinned;
        }

        match get_more_result {
            Ok(get_more) => {
                self.state_mut().buffer = CursorBuffer::new(get_more.batch);
                self.state_mut().post_batch_resume_token = get_more.post_batch_resume_token;

                Ok(())
            }
            Err(e) => {
                if e.is_network_error() {
                    // Flag the connection as invalid, preventing a killCursors command,
                    // but leave the connection pinned.
                    self.state_mut().pinned_connection.invalidate();
                }
                self.state_mut().error = Some(e.clone());

                Err(e)
            }
        }
    }

    pub(super) fn provider_mut(&mut self) -> &mut P {
        &mut self.provider
    }

    pub(super) fn with_type<'a, D>(self) -> GenericCursor<P, D>
    where
        D: Deserialize<'a>,
    {
        GenericCursor {
            client: self.client,
            provider: self.provider,
            info: self.info,
            state: self.state,
            _phantom: Default::default(),
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

impl<P, T> CursorStream for GenericCursor<P, T>
where
    P: GetMoreProvider,
    T: DeserializeOwned + Unpin,
{
    fn poll_next_in_batch(&mut self, cx: &mut Context<'_>) -> Poll<Result<BatchValue>> {
        // If there is a get more in flight, check on its status.
        if let Some(future) = self.provider.executing_future() {
            match Pin::new(future).poll(cx) {
                // If a result is ready, retrieve the buffer and update the exhausted status.
                Poll::Ready(get_more_result) => {
                    let (result, session) = get_more_result.into_parts();
                    let output = self.handle_get_more_result(result);
                    self.provider
                        .clear_execution(session, self.state.as_ref().unwrap().exhausted);
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
                    return Poll::Ready(Some(Ok(bson::from_slice(doc.as_bytes())?)))
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

impl<'a, C> Future for NextInBatchFuture<'a, C>
where
    C: CursorStream,
{
    type Output = Result<BatchValue>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_next_in_batch(cx)
    }
}

impl<P, T> Stream for GenericCursor<P, T>
where
    P: GetMoreProvider,
    T: DeserializeOwned + Unpin,
{
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        stream_poll_next(Pin::into_inner(self), cx)
    }
}

/// A trait implemented by objects that can provide batches of documents to a cursor via the getMore
/// command.
pub(super) trait GetMoreProvider: Unpin {
    /// The result type that the future running the getMore evaluates to.
    type ResultType: GetMoreProviderResult;

    /// The type of future created by this provider when running a getMore.
    type GetMoreFuture: Future<Output = Self::ResultType> + Unpin;

    /// Get the future being evaluated, if there is one.
    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture>;

    /// Clear out any state remaining from previous getMore executions.
    fn clear_execution(
        &mut self,
        session: <Self::ResultType as GetMoreProviderResult>::Session,
        exhausted: bool,
    );

    /// Start executing a new getMore if one isn't already in flight.
    fn start_execution(
        &mut self,
        spec: CursorInformation,
        client: Client,
        pinned_connection: Option<&PinnedConnectionHandle>,
    );

    /// Return a future that will execute the getMore when polled.
    /// This is useful in async functions that can await the entire getMore process.
    /// `start_execution` and `clear_execution` should be used for contexts where the futures
    /// need to be polled manually.
    fn execute(
        &mut self,
        _spec: CursorInformation,
        _client: Client,
        _pinned_conn: PinnedConnection,
    ) -> BoxFuture<'_, Result<GetMoreResult>>;
}

/// Trait describing results returned from a `GetMoreProvider`.
pub(crate) trait GetMoreProviderResult {
    type Session;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult, &Error>;

    fn into_parts(self) -> (Result<GetMoreResult>, Self::Session);

    /// Whether the response from the server indicated the cursor was exhausted or not.
    fn exhausted(&self) -> bool {
        match self.as_ref() {
            Ok(res) => res.exhausted,
            Err(e) => {
                matches!(*e.kind, ErrorKind::Command(ref e) if e.code == 43 || e.code == 237)
            }
        }
    }
}

/// Specification used to create a new cursor.
#[derive(Debug, Clone)]
pub(crate) struct CursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_buffer: VecDeque<RawDocumentBuf>,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
}

impl CursorSpecification {
    pub(crate) fn new(
        info: operation::CursorInfo,
        address: ServerAddress,
        batch_size: impl Into<Option<u32>>,
        max_time: impl Into<Option<Duration>>,
    ) -> Self {
        Self {
            info: CursorInformation {
                ns: info.ns,
                id: info.id,
                address,
                batch_size: batch_size.into(),
                max_time: max_time.into(),
            },
            initial_buffer: info.first_batch,
            post_batch_resume_token: ResumeToken::from_raw(info.post_batch_resume_token),
        }
    }

    pub(crate) fn id(&self) -> i64 {
        self.info.id
    }

    #[cfg(test)]
    pub(crate) fn address(&self) -> &ServerAddress {
        &self.info.address
    }

    #[cfg(test)]
    pub(crate) fn batch_size(&self) -> Option<u32> {
        self.info.batch_size
    }

    #[cfg(test)]
    pub(crate) fn max_time(&self) -> Option<Duration> {
        self.info.max_time
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
    ns: &Namespace,
    cursor_id: i64,
    pinned_conn: PinnedConnection,
    drop_address: Option<ServerAddress>,
    #[cfg(test)] kill_watcher: Option<oneshot::Sender<()>>,
) {
    let coll = client
        .database(ns.db.as_str())
        .collection::<Document>(ns.coll.as_str());
    RUNTIME.execute(async move {
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
    pub(crate) error: Option<Error>,
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

    pub(crate) fn next(&mut self) -> Option<RawDocumentBuf> {
        self.fresh = false;
        self.docs.pop_front()
    }

    pub(crate) fn advance(&mut self) {
        // if at the front of the buffer, don't move forward as the first document
        // hasn't been consumed yet.
        if self.fresh {
            self.fresh = false;
            return;
        }
        self.next();
    }

    pub(crate) fn current(&self) -> Option<&RawDocument> {
        self.docs.front().map(|d| d.as_ref())
    }
}
