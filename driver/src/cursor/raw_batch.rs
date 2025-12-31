//! Raw batch cursor API for zero-copy document processing.
//!
//! This module provides a high-performance alternative to the standard cursor API when you need
//! direct access to server response batches without per-document deserialization overhead.
//!
//! # When to Use
//!
//! **Use `find_raw_batches()` when:**
//! - Processing high-volume queries where deserialization is a bottleneck
//! - Implementing custom batch-level logic (e.g., batch transformation, filtering)
//! - Inspecting raw BSON structure without a known schema
//! - Forwarding documents without modification (e.g., proxying, caching)
//!
//! **Use regular `find()` when:**
//! - Working with strongly-typed `Deserialize` documents
//! - Iterating one document at a time
//! - Deserialization overhead is acceptable for your use case
//!
//! # Example
//!
//! ```no_run
//! # use mongodb::{Client, bson::doc};
//! # async fn example() -> mongodb::error::Result<()> {
//! # let client = Client::with_uri_str("mongodb://localhost:27017").await?;
//! # let db = client.database("db");
//! use futures::stream::StreamExt;
//!
//! let mut cursor = db.find_raw_batches("coll", doc! {}).await?;
//! while let Some(batch) = cursor.next().await {
//!     let batch = batch?;
//!     // Zero-copy access to documents in this batch
//!     for doc_result in batch.doc_slices()? {
//!         let doc = doc_result?;
//!         // Process raw document
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    bson::{RawArray, RawBsonRef, RawDocument},
    operation::GetMore,
};
use futures_core::{future::BoxFuture, Future, Stream};

use crate::{
    bson::RawDocumentBuf,
    change_stream::event::ResumeToken,
    client::{options::ServerAddress, AsyncDropToken},
    cmap::conn::PinnedConnectionHandle,
    cursor::common::{
        kill_cursor,
        ClientSessionHandle,
        ExplicitClientSessionHandle,
        ImplicitClientSessionHandle,
        PinnedConnection,
    },
    error::{Error, ErrorKind, Result},
    Client,
    ClientSession,
};

use super::common::CursorInformation;

#[derive(Debug, Clone)]
pub(crate) struct RawBatchCursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_reply: RawDocumentBuf,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
}

/// A raw batch response returned by the server for a cursor getMore/find.
///
/// This provides zero-copy access to the server's batch array via
/// [`doc_slices`](RawBatch::doc_slices).
#[derive(Debug)]
pub struct RawBatch {
    reply: RawDocumentBuf,
}

impl RawBatch {
    pub(crate) fn new(reply: RawDocumentBuf) -> Self {
        Self { reply }
    }

    /// Returns a borrowed view of the batch array (`firstBatch` or `nextBatch`) without copying.
    ///
    /// This lets callers iterate over [`crate::bson::RawDocument`] items directly for maximal
    /// performance.
    pub fn doc_slices(&self) -> Result<&RawArray> {
        let root = self.reply.as_ref();
        let cursor = root
            .get("cursor")?
            .and_then(RawBsonRef::as_document)
            .ok_or_else(|| Error::invalid_response("missing cursor subdocument"))?;

        let docs = cursor
            .get("firstBatch")?
            .or_else(|| cursor.get("nextBatch").ok().flatten())
            .ok_or_else(|| Error::invalid_response("missing firstBatch/nextBatch"))?;

        docs.as_array()
            .ok_or_else(|| Error::invalid_response("missing firstBatch/nextBatch"))
    }

    /// Returns a reference to the full server response document.
    ///
    /// This provides access to all fields in the server's response, including cursor metadata,
    /// for debugging or custom parsing.
    pub fn as_raw_document(&self) -> &RawDocument {
        self.reply.as_ref()
    }
}

pub struct RawBatchCursor {
    client: Client,
    drop_token: AsyncDropToken,
    info: CursorInformation,
    state: RawBatchCursorState,
    drop_address: Option<ServerAddress>,
}

struct RawBatchCursorState {
    exhausted: bool,
    pinned_connection: PinnedConnection,
    post_batch_resume_token: Option<ResumeToken>,
    provider: GetMoreRawProvider<'static, ImplicitClientSessionHandle>,
    initial_reply: Option<RawDocumentBuf>,
}

impl RawBatchCursor {
    pub(crate) fn new(
        client: Client,
        spec: RawBatchCursorSpecification,
        session: Option<ClientSession>,
        pin: Option<PinnedConnectionHandle>,
    ) -> Self {
        let exhausted = spec.info.id == 0;
        Self {
            client: client.clone(),
            drop_token: client.register_async_drop(),
            info: spec.info,
            drop_address: None,
            state: RawBatchCursorState {
                exhausted,
                pinned_connection: PinnedConnection::new(pin),
                post_batch_resume_token: spec.post_batch_resume_token,
                provider: if exhausted {
                    GetMoreRawProvider::Done
                } else {
                    GetMoreRawProvider::Idle(Box::new(ImplicitClientSessionHandle(session)))
                },
                initial_reply: Some(spec.initial_reply),
            },
        }
    }

    pub fn address(&self) -> &ServerAddress {
        &self.info.address
    }

    pub fn set_drop_address(&mut self, address: ServerAddress) {
        self.drop_address = Some(address);
    }

    pub fn is_exhausted(&self) -> bool {
        self.state.exhausted
    }

    fn mark_exhausted(&mut self) {
        self.state.exhausted = true;
        self.state.pinned_connection = PinnedConnection::Unpinned;
    }
}

impl Stream for RawBatchCursor {
    type Item = Result<RawBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If a getMore is in flight, poll it and update state.
            if let Some(future) = self.state.provider.executing_future() {
                match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(get_more_out) => {
                        match get_more_out.result {
                            Ok(out) => {
                                self.state.initial_reply = Some(out.raw_reply);
                                self.state.post_batch_resume_token = out.post_batch_resume_token;
                                if out.exhausted {
                                    self.mark_exhausted();
                                }
                                if out.id != 0 {
                                    self.info.id = out.id;
                                }
                                self.info.ns = out.ns;
                            }
                            Err(e) => {
                                if matches!(*e.kind, ErrorKind::Command(ref ce) if ce.code == 43 || ce.code == 237)
                                {
                                    self.mark_exhausted();
                                }
                                let exhausted_now = self.state.exhausted;
                                self.state
                                    .provider
                                    .clear_execution(get_more_out.session, exhausted_now);
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        let exhausted_now = self.state.exhausted;
                        self.state
                            .provider
                            .clear_execution(get_more_out.session, exhausted_now);
                    }
                }
            }

            // Yield any buffered reply.
            if let Some(reply) = self.state.initial_reply.take() {
                return Poll::Ready(Some(Ok(RawBatch::new(reply))));
            }

            // If not exhausted and the connection is valid, start a getMore and iterate.
            if !self.state.exhausted
                && !matches!(self.state.pinned_connection, PinnedConnection::Invalid(_))
            {
                let info = self.info.clone();
                let client = self.client.clone();
                let state = &mut self.state;
                state
                    .provider
                    .start_execution(info, client, state.pinned_connection.handle());
                continue;
            }

            // Otherwise, we're done.
            return Poll::Ready(None);
        }
    }
}

impl Drop for RawBatchCursor {
    fn drop(&mut self) {
        if self.is_exhausted() {
            return;
        }
        kill_cursor(
            self.client.clone(),
            &mut self.drop_token,
            &self.info.ns,
            self.info.id,
            self.state.pinned_connection.replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            None,
        );
    }
}

#[derive(Debug)]
pub struct SessionRawBatchCursor {
    client: Client,
    drop_token: AsyncDropToken,
    info: CursorInformation,
    exhausted: bool,
    pinned_connection: PinnedConnection,
    post_batch_resume_token: Option<ResumeToken>,
    initial_reply: Option<RawDocumentBuf>,
    drop_address: Option<ServerAddress>,
}

impl SessionRawBatchCursor {
    pub(crate) fn new(
        client: Client,
        spec: RawBatchCursorSpecification,
        pinned: Option<PinnedConnectionHandle>,
    ) -> Self {
        let exhausted = spec.info.id == 0;
        Self {
            drop_token: client.register_async_drop(),
            client,
            info: spec.info,
            exhausted,
            pinned_connection: PinnedConnection::new(pinned),
            post_batch_resume_token: spec.post_batch_resume_token,
            initial_reply: Some(spec.initial_reply),
            drop_address: None,
        }
    }

    pub fn stream<'session>(
        &mut self,
        session: &'session mut ClientSession,
    ) -> SessionRawBatchCursorStream<'_, 'session> {
        SessionRawBatchCursorStream {
            parent: self,
            provider: GetMoreRawProvider::Idle(Box::new(ExplicitClientSessionHandle(session))),
        }
    }

    pub fn address(&self) -> &ServerAddress {
        &self.info.address
    }

    pub fn set_drop_address(&mut self, address: ServerAddress) {
        self.drop_address = Some(address);
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }
}

impl Drop for SessionRawBatchCursor {
    fn drop(&mut self) {
        if self.is_exhausted() {
            return;
        }
        kill_cursor(
            self.client.clone(),
            &mut self.drop_token,
            &self.info.ns,
            self.info.id,
            self.pinned_connection.replicate(),
            self.drop_address.take(),
            #[cfg(test)]
            None,
        );
    }
}

pub struct SessionRawBatchCursorStream<'cursor, 'session> {
    parent: &'cursor mut SessionRawBatchCursor,
    provider: GetMoreRawProvider<'session, ExplicitClientSessionHandle<'session>>,
}

impl Stream for SessionRawBatchCursorStream<'_, '_> {
    type Item = Result<RawBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // If a getMore is in flight, poll it and update state.
            if let Some(future) = self.provider.executing_future() {
                match Pin::new(future).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(get_more_out) => {
                        match get_more_out.result {
                            Ok(out) => {
                                if out.exhausted {
                                    self.parent.exhausted = true;
                                }
                                if out.id != 0 {
                                    self.parent.info.id = out.id;
                                }
                                self.parent.info.ns = out.ns;
                                self.parent.post_batch_resume_token = out.post_batch_resume_token;
                                // Buffer next reply to yield on following polls.
                                self.parent.initial_reply = Some(out.raw_reply);
                            }
                            Err(e) => {
                                if matches!(*e.kind, ErrorKind::Command(ref ce) if ce.code == 43 || ce.code == 237)
                                {
                                    self.parent.exhausted = true;
                                }
                                let exhausted_now = self.parent.exhausted;
                                self.provider
                                    .clear_execution(get_more_out.session, exhausted_now);
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        let exhausted_now = self.parent.exhausted;
                        self.provider
                            .clear_execution(get_more_out.session, exhausted_now);
                    }
                }
            }

            // Yield any buffered reply.
            if let Some(reply) = self.parent.initial_reply.take() {
                return Poll::Ready(Some(Ok(RawBatch::new(reply))));
            }

            // If not exhausted and the connection is valid, start a getMore and iterate.
            if !self.parent.exhausted
                && !matches!(self.parent.pinned_connection, PinnedConnection::Invalid(_))
            {
                let info = self.parent.info.clone();
                let client = self.parent.client.clone();
                let pinned_owned = self
                    .parent
                    .pinned_connection
                    .handle()
                    .map(|c| c.replicate());
                let pinned_ref = pinned_owned.as_ref();
                self.provider.start_execution(info, client, pinned_ref);
                continue;
            }

            // Otherwise, we're done.
            return Poll::Ready(None);
        }
    }
}
pub struct GetMoreRawResultAndSession<S> {
    pub result: Result<crate::results::GetMoreResult>,
    pub session: S,
}

enum GetMoreRawProvider<'s, S> {
    Executing(BoxFuture<'s, GetMoreRawResultAndSession<S>>),
    Idle(Box<S>),
    Done,
}

impl<'s, S: ClientSessionHandle<'s>> GetMoreRawProvider<'s, S> {
    fn executing_future(&mut self) -> Option<&mut BoxFuture<'s, GetMoreRawResultAndSession<S>>> {
        if let Self::Executing(future) = self {
            Some(future)
        } else {
            None
        }
    }

    fn clear_execution(&mut self, session: S, exhausted: bool) {
        if exhausted && session.is_implicit() {
            *self = Self::Done
        } else {
            *self = Self::Idle(Box::new(session))
        }
    }

    fn start_execution(
        &mut self,
        info: CursorInformation,
        client: Client,
        pinned_connection: Option<&PinnedConnectionHandle>,
    ) {
        take_mut::take(self, |this| {
            if let Self::Idle(mut session) = this {
                let pinned = pinned_connection.map(|c| c.replicate());
                let fut = Box::pin(async move {
                    let get_more = GetMore::new_owned(info, pinned.as_ref(), true);
                    let res = client
                        .execute_operation(get_more, session.borrow_mut())
                        .await;
                    GetMoreRawResultAndSession {
                        result: res,
                        session: *session,
                    }
                });
                Self::Executing(fut)
            } else {
                this
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bson::{doc, Document};

    #[test]
    fn raw_batch_into_docs_works() {
        let reply_doc: Document = doc! {
            "ok": 1,
            "cursor": {
                "id": 0_i64,
                "ns": "db.coll",
                "firstBatch": [
                    { "x": 1 },
                    { "x": 2 }
                ]
            }
        };
        let mut bytes = Vec::new();
        reply_doc.to_writer(&mut bytes).unwrap();
        let raw = RawDocumentBuf::from_bytes(bytes).unwrap();

        let batch = RawBatch::new(raw);
        let docs: Vec<_> = batch.doc_slices().unwrap().into_iter().collect();
        assert_eq!(docs.len(), 2);
    }
}
