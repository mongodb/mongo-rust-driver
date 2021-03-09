use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use derivative::Derivative;
use futures::{Future, Stream};

use crate::{
    bson::Document,
    error::{Error, ErrorKind, Result},
    options::StreamAddress,
    results::GetMoreResult,
    Client,
    Namespace,
};

/// An internal cursor that can be used in a variety of contexts depending on its `GetMoreProvider`.
#[derive(Derivative)]
#[derivative(Debug)]
pub(super) struct GenericCursor<T: GetMoreProvider> {
    #[derivative(Debug = "ignore")]
    provider: T,
    client: Client,
    info: CursorInformation,
    buffer: VecDeque<Document>,
    exhausted: bool,
}

impl<T: GetMoreProvider> GenericCursor<T> {
    pub(super) fn new(client: Client, spec: CursorSpecification, get_more_provider: T) -> Self {
        let exhausted = spec.id() == 0;
        Self {
            exhausted,
            client,
            provider: get_more_provider,
            buffer: spec.initial_buffer,
            info: spec.info,
        }
    }

    pub(super) fn take_buffer(&mut self) -> VecDeque<Document> {
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

    fn start_get_more(&mut self) {
        let info = self.info.clone();
        let client = self.client.clone();
        self.provider.start_execution(info, client);
    }
}

impl<T: GetMoreProvider> Stream for GenericCursor<T> {
    type Item = Result<Document>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If there is a get more in flight, check on its status.
            if let Some(future) = self.provider.executing_future() {
                match Pin::new(future).poll(cx) {
                    // If a result is ready, retrieve the buffer and update the exhausted status.
                    Poll::Ready(get_more_result) => {
                        let exhausted = get_more_result.exhausted();
                        let (result, session) = get_more_result.into_parts();

                        self.exhausted = exhausted;
                        self.provider.clear_execution(session, exhausted);
                        self.buffer = result?.batch;
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match self.buffer.pop_front() {
                Some(doc) => {
                    if self.buffer.is_empty() && !self.exhausted {
                        self.start_get_more();
                    }
                    return Poll::Ready(Some(Ok(doc)));
                }
                None if !self.exhausted => {
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
    type GetMoreResult: GetMoreProviderResult;

    /// The type of future created by this provider when running a getMore.
    type GetMoreFuture: Future<Output = Self::GetMoreResult> + Unpin;

    /// Get the future being evaluated, if there is one.
    fn executing_future(&mut self) -> Option<&mut Self::GetMoreFuture>;

    /// Clear out any state remaining from previous getMore executions.
    fn clear_execution(
        &mut self,
        session: <Self::GetMoreResult as GetMoreProviderResult>::Session,
        exhausted: bool,
    );

    /// Start executing a new getMore if one isn't already in flight.
    fn start_execution(&mut self, spec: CursorInformation, client: Client);
}

/// Trait describing results returned from a `GetMoreProvider`.
pub(super) trait GetMoreProviderResult {
    type Session;

    fn as_ref(&self) -> std::result::Result<&GetMoreResult, &Error>;

    fn into_parts(self) -> (Result<GetMoreResult>, Self::Session);

    /// Whether the response from the server indicated the cursor was exhausted or not.
    fn exhausted(&self) -> bool {
        match self.as_ref() {
            Ok(res) => res.exhausted,
            Err(e) => {
                matches!(e.kind, ErrorKind::CommandError(ref e) if e.code == 43 || e.code == 237)
            }
        }
    }
}

/// Specification used to create a new cursor.
#[derive(Debug, Clone)]
pub(crate) struct CursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_buffer: VecDeque<Document>,
}

impl CursorSpecification {
    pub(crate) fn new(
        ns: Namespace,
        address: StreamAddress,
        id: i64,
        batch_size: impl Into<Option<u32>>,
        max_time: impl Into<Option<Duration>>,
        initial_buffer: VecDeque<Document>,
    ) -> Self {
        Self {
            info: CursorInformation {
                ns,
                id,
                address,
                batch_size: batch_size.into(),
                max_time: max_time.into(),
            },
            initial_buffer,
        }
    }

    pub(crate) fn id(&self) -> i64 {
        self.info.id
    }

    #[cfg(test)]
    pub(crate) fn address(&self) -> &StreamAddress {
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
    pub(crate) address: StreamAddress,
    pub(crate) id: i64,
    pub(crate) batch_size: Option<u32>,
    pub(crate) max_time: Option<Duration>,
}
