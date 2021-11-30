use std::{
    collections::VecDeque,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bson::RawDocumentBuf;
use derivative::Derivative;
use futures_core::{Future, Stream};
use serde::de::DeserializeOwned;
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
    buffer: VecDeque<RawDocumentBuf>,
    exhausted: bool,
    pinned_connection: PinnedConnection,
    resume_token: Option<ResumeToken>,
    _phantom: PhantomData<T>,
}

impl<P, T> GenericCursor<P, T>
where
    P: GetMoreProvider,
    T: DeserializeOwned,
{
    pub(super) fn new(
        client: Client,
        spec: CursorSpecification,
        pinned_connection: PinnedConnection,
        resume_token: Option<ResumeToken>,
        get_more_provider: P,
    ) -> Self {
        let exhausted = spec.id() == 0;
        Self {
            exhausted,
            client,
            provider: get_more_provider,
            buffer: spec.initial_buffer,
            info: spec.info,
            pinned_connection,
            resume_token,
            _phantom: Default::default(),
        }
    }

    pub(super) fn take_buffer(&mut self) -> VecDeque<RawDocumentBuf> {
        std::mem::take(&mut self.buffer)
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub(super) fn id(&self) -> i64 {
        self.info.id
    }

    pub(super) fn namespace(&self) -> &Namespace {
        &self.info.ns
    }

    pub(super) fn pinned_connection(&self) -> &PinnedConnection {
        &self.pinned_connection
    }

    pub(super) fn resume_token(&self) -> Option<&ResumeToken> {
        self.resume_token.as_ref()
    }

    fn start_get_more(&mut self) {
        let info = self.info.clone();
        let client = self.client.clone();
        self.provider
            .start_execution(info, client, self.pinned_connection.handle());
    }

    pub(super) fn with_type<D: DeserializeOwned>(self) -> GenericCursor<P, D> {
        GenericCursor {
            exhausted: self.exhausted,
            client: self.client,
            provider: self.provider,
            buffer: self.buffer,
            info: self.info,
            pinned_connection: self.pinned_connection,
            resume_token: self.resume_token,
            _phantom: Default::default(),
        }
    }
}

impl<P, T> Stream for GenericCursor<P, T>
where
    P: GetMoreProvider,
    T: DeserializeOwned + Unpin,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If there is a get more in flight, check on its status.
            if let Some(future) = self.provider.executing_future() {
                match Pin::new(future).poll(cx) {
                    // If a result is ready, retrieve the buffer and update the exhausted status.
                    Poll::Ready(get_more_result) => {
                        let exhausted = get_more_result.exhausted();
                        let (result, session) = get_more_result.into_parts();
                        if exhausted {
                            // If the cursor is exhausted, the driver must return the pinned
                            // connection to the pool.
                            self.pinned_connection = PinnedConnection::Unpinned;
                        }
                        if let Err(e) = &result {
                            if e.is_network_error() {
                                // Flag the connection as invalid, preventing a killCursors command,
                                // but leave the connection pinned.
                                self.pinned_connection.invalidate();
                            }
                        }

                        self.exhausted = exhausted;
                        self.provider.clear_execution(session, exhausted);
                        self.buffer = result?.batch;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match self.buffer.pop_front() {
                Some(doc) => {
                    return Poll::Ready(Some(Ok(bson::from_slice(doc.as_bytes())?)));
                }
                None if !self.exhausted && !self.pinned_connection.is_invalid() => {
                    self.start_get_more();
                }
                None => return Poll::Ready(None),
            }
        }
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
    pub(crate) post_batch_resume_token: Option<Document>,
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
            post_batch_resume_token: info.post_batch_resume_token,
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

    pub(crate) fn take(&mut self) -> PinnedConnection {
        let out = self.replicate();
        *self = Self::Unpinned;
        out
    }

    fn handle(&self) -> Option<&PinnedConnectionHandle> {
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
    #[cfg(test)] kill_watcher: Option<oneshot::Sender<()>>,
) {
    let coll = client
        .database(ns.db.as_str())
        .collection::<Document>(ns.coll.as_str());
    RUNTIME.execute(async move {
        if !pinned_conn.is_invalid() {
            let _ = coll.kill_cursor(cursor_id, pinned_conn.handle()).await;
            #[cfg(test)]
            if let Some(tx) = kill_watcher {
                let _ = tx.send(());
            }
        }
    });
}
