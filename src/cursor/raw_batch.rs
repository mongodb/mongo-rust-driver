use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::bson::{RawArray, RawBsonRef};
use futures_core::{future::BoxFuture, Future, Stream};

use crate::{
    bson::{RawDocument, RawDocumentBuf},
    change_stream::event::ResumeToken,
    client::{options::ServerAddress, AsyncDropToken},
    cmap::conn::PinnedConnectionHandle,
    cursor::common::{
        kill_cursor, ClientSessionHandle, ExplicitClientSessionHandle, ImplicitClientSessionHandle,
        PinnedConnection,
    },
    error::{Error, ErrorKind, Result},
    operation::get_more_raw::GetMoreRaw,
    Client, ClientSession,
};

use super::common::CursorInformation;

#[derive(Debug, Clone)]
pub(crate) struct RawBatchCursorSpecification {
    pub(crate) info: CursorInformation,
    pub(crate) initial_reply: RawDocumentBuf,
    pub(crate) post_batch_resume_token: Option<ResumeToken>,
}

#[derive(Debug)]
pub struct RawBatch {
    reply: RawDocumentBuf,
}

impl RawBatch {
    pub(crate) fn new(reply: RawDocumentBuf) -> Self {
        Self { reply }
    }

    pub fn doc_slices<'a>(&'a self) -> Result<&'a RawArray> {
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
    pending_reply: Option<RawDocumentBuf>,
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
                pending_reply: None,
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
                                self.state.pending_reply = Some(out.raw_reply);
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

            // Yield any buffered reply (initial or pending).
            if let Some(reply) = self
                .state
                .initial_reply
                .take()
                .or_else(|| self.state.pending_reply.take())
            {
                // Prefetch the next getMore before returning this batch, if applicable.
                if !self.state.exhausted
                    && !matches!(self.state.pinned_connection, PinnedConnection::Invalid(_))
                {
                    let info = self.info.clone();
                    let client = self.client.clone();
                    let state = &mut self.state;
                    state
                        .provider
                        .start_execution(info, client, state.pinned_connection.handle());
                    // Immediately poll once to register the waker and opportunistically buffer.
                    if let Some(fut) = state.provider.executing_future() {
                        match Pin::new(fut).poll(cx) {
                            Poll::Pending => {}
                            Poll::Ready(get_more_out) => {
                                match get_more_out.result {
                                    Ok(out) => {
                                        state.pending_reply = Some(out.raw_reply);
                                        state.post_batch_resume_token = out.post_batch_resume_token;
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
                                        // Intentionally do not surface the error here; clear and continue.
                                    }
                                }
                                let exhausted_now = self.state.exhausted;
                                self.state
                                    .provider
                                    .clear_execution(get_more_out.session, exhausted_now);
                            }
                        }
                    }
                }
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

            // Yield any buffered reply (initial).
            if let Some(reply) = self.parent.initial_reply.take() {
                // Prefetch the next getMore before returning this batch, if applicable.
                if !self.parent.exhausted
                    && !matches!(self.parent.pinned_connection, PinnedConnection::Invalid(_))
                {
                    let info = self.parent.info.clone();
                    let client = self.parent.client.clone();
                    // Avoid borrow conflicts by replicating the handle into a temporary owner.
                    let pinned_owned = self
                        .parent
                        .pinned_connection
                        .handle()
                        .map(|c| c.replicate());
                    let pinned_ref = pinned_owned.as_ref();
                    self.provider.start_execution(info, client, pinned_ref);
                    // Immediately poll once to register the waker and opportunistically buffer.
                    if let Some(fut) = self.provider.executing_future() {
                        match Pin::new(fut).poll(cx) {
                            Poll::Pending => {}
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
                                        self.parent.post_batch_resume_token =
                                            out.post_batch_resume_token;
                                        // buffer for the next poll
                                        self.parent.initial_reply = Some(out.raw_reply);
                                    }
                                    Err(e) => {
                                        if matches!(*e.kind, ErrorKind::Command(ref ce) if ce.code == 43 || ce.code == 237)
                                        {
                                            self.parent.exhausted = true;
                                        }
                                        // Intentionally do not surface the error here; clear and continue.
                                    }
                                }
                                let exhausted_now = self.parent.exhausted;
                                self.provider
                                    .clear_execution(get_more_out.session, exhausted_now);
                            }
                        }
                    }
                }
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
    pub result: Result<crate::operation::get_more_raw::GetMoreRawResult>,
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
                    let get_more = GetMoreRaw::new(info, pinned.as_ref());
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

pub struct RawDocumentStream<R> {
    inner: R,
}

impl RawBatchCursor {
    pub fn into_raw_documents(self) -> RawDocumentStream<Self> {
        RawDocumentStream { inner: self }
    }
}

impl Stream for RawDocumentStream<RawBatchCursor> {
    type Item = Result<RawBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
        }
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
