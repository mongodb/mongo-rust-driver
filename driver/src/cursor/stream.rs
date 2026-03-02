use std::{collections::VecDeque, task::Poll};

use derive_where::derive_where;
use futures_core::Stream as AsyncStream;
use futures_util::{stream::StreamExt, FutureExt};
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    bson::{RawDocument, RawDocumentBuf},
    error::{Error, Result},
    BoxFuture,
};

use super::raw_batch::RawBatch;

/// `Stream` represents an "introspectable" cursor stream - an implementation of an async `Stream`
/// with a buffer that's available for external use when the stream isn't actively being polled.
///
/// If the buffer *is* queried during a poll, it will cause a panic.  This will only happen if a
/// future is dropped without being fully polled, which is documented as unsupported by the driver.
#[derive_where(Debug)]
pub(super) struct Stream<'a, Raw, T> {
    state: StreamState<'a, Raw>,
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<'a, Raw, T> Stream<'a, Raw, T> {
    pub(super) fn new(raw: Raw) -> Self {
        Self::from_cursor(BatchBuffer::new(raw))
    }

    pub(super) fn from_cursor(cs: BatchBuffer<Raw>) -> Self {
        Self {
            state: StreamState::Idle(cs),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn buffer(&self) -> &BatchBuffer<Raw> {
        match &self.state {
            StreamState::Idle(state) => state,
            _ => panic!("state access while streaming"),
        }
    }

    pub(super) fn buffer_mut(&mut self) -> &mut BatchBuffer<Raw> {
        match &mut self.state {
            StreamState::Idle(state) => state,
            _ => panic!("state access while streaming"),
        }
    }

    pub(super) fn take_buffer(&mut self) -> BatchBuffer<Raw> {
        match std::mem::replace(&mut self.state, StreamState::Polling) {
            StreamState::Idle(state) => state,
            _ => panic!("state access while streaming"),
        }
    }

    pub(super) fn with_type<D>(self) -> Stream<'a, Raw, D> {
        Stream {
            state: self.state,
            _phantom: std::marker::PhantomData,
        }
    }
}

#[derive_where(Debug)]
enum StreamState<'a, Raw> {
    Idle(BatchBuffer<Raw>),
    Polling,
    Advance(#[derive_where(skip)] BoxFuture<'a, AdvanceDone<Raw>>),
}

#[derive_where(Debug)]
struct AdvanceDone<Raw> {
    buffer: BatchBuffer<Raw>,
    out: Result<bool>,
}

#[derive_where(Debug)]
pub(super) struct BatchBuffer<Raw> {
    #[derive_where(skip)]
    pub(super) raw: Raw,
    batch: VecDeque<RawDocumentBuf>,
}

impl<Raw> BatchBuffer<Raw> {
    pub(super) fn new(raw: Raw) -> Self {
        Self {
            raw,
            batch: VecDeque::new(),
        }
    }

    pub(super) fn current(&self) -> &RawDocument {
        self.batch.front().unwrap()
    }

    pub(super) fn deserialize_current<'a, V>(&'a self) -> Result<V>
    where
        V: Deserialize<'a>,
    {
        crate::bson_compat::deserialize_from_slice(self.current().as_bytes()).map_err(Error::from)
    }

    pub(super) fn map<G>(self, f: impl FnOnce(Raw) -> G) -> BatchBuffer<G> {
        BatchBuffer {
            raw: f(self.raw),
            batch: self.batch,
        }
    }

    pub(crate) fn batch(&self) -> &VecDeque<RawDocumentBuf> {
        &self.batch
    }
}

impl<Raw: AsyncStream<Item = Result<RawBatch>> + Unpin> BatchBuffer<Raw> {
    /// Attempt to advance the cursor forward to the next item. If there are no items cached
    /// locally, perform getMores until the cursor is exhausted or the buffer has been refilled.
    /// Return whether or not the cursor has been advanced.
    pub(super) async fn advance(&mut self) -> Result<bool> {
        loop {
            match self.advance_internal().await? {
                AdvanceResult::Advanced => return Ok(true),
                AdvanceResult::Exhausted => return Ok(false),
                AdvanceResult::Waiting => continue,
            }
        }
    }

    /// Attempt to advance the cursor forward to the next item. If there are no items cached
    /// locally, perform a single getMore to attempt to retrieve more.
    pub(super) async fn try_advance(&mut self) -> Result<bool> {
        self.advance_internal()
            .await
            .map(|ar| matches!(ar, AdvanceResult::Advanced))
    }

    async fn advance_internal(&mut self) -> Result<AdvanceResult> {
        // Next stored batch item
        self.batch.pop_front();
        if !self.batch.is_empty() {
            return Ok(AdvanceResult::Advanced);
        }

        // Batch is empty, need a new one
        let Some(raw_batch) = self.raw.next().await else {
            return Ok(AdvanceResult::Exhausted);
        };
        let raw_batch = raw_batch?;
        for item in raw_batch.doc_slices()? {
            self.batch.push_back(
                item?
                    .as_document()
                    .ok_or_else(|| Error::invalid_response("invalid cursor batch item"))?
                    .to_owned(),
            );
        }
        return Ok(if self.batch.is_empty() {
            AdvanceResult::Waiting
        } else {
            AdvanceResult::Advanced
        });
    }
}

/// The result of one attempt to advance a cursor.
#[derive(Debug)]
enum AdvanceResult {
    /// The cursor was successfully advanced and the buffer has at least one item.
    Advanced,
    /// The cursor does not have any more items and will not return any more in the future.
    Exhausted,
    /// The cursor does not currently have any items, but future calls to getMore may yield more.
    Waiting,
}

impl<'a, Raw: 'a + AsyncStream<Item = Result<RawBatch>> + Send + Unpin, T: DeserializeOwned>
    AsyncStream for Stream<'a, Raw, T>
{
    type Item = Result<T>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match std::mem::replace(&mut self.state, StreamState::Polling) {
                StreamState::Idle(mut buffer) => {
                    self.state = StreamState::Advance(
                        async move {
                            let out = buffer.advance().await;
                            AdvanceDone { buffer, out }
                        }
                        .boxed(),
                    );
                    continue;
                }
                StreamState::Advance(mut fut) => {
                    return match fut.poll_unpin(cx) {
                        Poll::Pending => {
                            self.state = StreamState::Advance(fut);
                            Poll::Pending
                        }
                        Poll::Ready(ar) => {
                            let out = match ar.out {
                                Err(e) => Some(Err(e)),
                                Ok(false) => None,
                                Ok(true) => Some(ar.buffer.deserialize_current()),
                            };
                            self.state = StreamState::Idle(ar.buffer);
                            return Poll::Ready(out);
                        }
                    }
                }
                StreamState::Polling => {
                    return Poll::Ready(Some(Err(Error::internal(
                        "attempt to poll cursor already in polling state",
                    ))))
                }
            }
        }
    }
}
