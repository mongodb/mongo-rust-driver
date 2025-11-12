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

    pub fn raw_reply(&self) -> &RawDocument {
        self.reply.as_ref()
    }

    pub fn into_raw_reply(self) -> RawDocumentBuf {
        self.reply
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
        // Yield initial batch first, if present.
        if let Some(initial) = self.state.initial_reply.take() {
            // Prefetch the next getMore in the background, if applicable.
            if !self.state.exhausted {
                let info = self.info.clone();
                let client = self.client.clone();
                let pinned_owned = self.state.pinned_connection.handle().map(|c| c.replicate());
                let pinned_ref = pinned_owned.as_ref();
                self.state
                    .provider
                    .start_execution(info, client, pinned_ref);
                // Immediately poll once to register waker; if already ready, buffer the result.
                if let Some(f) = self.state.provider.executing_future() {
                    match Pin::new(f).poll(cx) {
                        Poll::Pending => {}
                        Poll::Ready(get_more_out) => {
                            match get_more_out.result {
                                Ok(out) => {
                                    self.state.pending_reply = Some(out.raw_reply);
                                    self.state.post_batch_resume_token =
                                        out.post_batch_resume_token;
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
            return Poll::Ready(Some(Ok(RawBatch::new(initial))));
        }

        // If a getMore is in flight, poll it.
        let mut ready = None;
        {
            let provider = &mut self.state.provider;
            if let Some(f) = provider.executing_future() {
                match Pin::new(f).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(g) => ready = Some(g),
                }
            }
        }
        if let Some(get_more_out) = ready {
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

        if let Some(reply) = self.state.pending_reply.take() {
            // Prefetch the next getMore before returning this batch, if applicable.
            if !self.state.exhausted {
                let info = self.info.clone();
                let client = self.client.clone();
                let pinned_owned = self.state.pinned_connection.handle().map(|c| c.replicate());
                let pinned_ref = pinned_owned.as_ref();
                self.state
                    .provider
                    .start_execution(info, client, pinned_ref);
                // Immediately poll once to register waker; if already ready, buffer the result.
                if let Some(f) = self.state.provider.executing_future() {
                    match Pin::new(f).poll(cx) {
                        Poll::Pending => {}
                        Poll::Ready(get_more_out) => {
                            match get_more_out.result {
                                Ok(out) => {
                                    self.state.pending_reply = Some(out.raw_reply);
                                    self.state.post_batch_resume_token =
                                        out.post_batch_resume_token;
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

        if !self.state.exhausted {
            let info = self.info.clone();
            let client = self.client.clone();
            let state = &mut self.state;
            state
                .provider
                .start_execution(info, client, state.pinned_connection.handle());
            // Immediately poll the newly-started getMore once to register the waker.
            if let Some(f) = state.provider.executing_future() {
                match Pin::new(f).poll(cx) {
                    Poll::Pending => return Poll::Pending,
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
                        if let Some(reply) = self.state.pending_reply.take() {
                            return Poll::Ready(Some(Ok(RawBatch::new(reply))));
                        } else if self.state.exhausted {
                            return Poll::Ready(None);
                        } else {
                            return Poll::Pending;
                        }
                    }
                }
            } else {
                return Poll::Pending;
            }
        }

        Poll::Ready(None)
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
        // yield initial reply first
        if let Some(initial) = self.parent.initial_reply.take() {
            // Prefetch the next getMore in the background, if applicable.
            if !self.parent.exhausted {
                let info = self.parent.info.clone();
                let client = self.parent.client.clone();
                let pinned_owned = self
                    .parent
                    .pinned_connection
                    .handle()
                    .map(|c| c.replicate());
                let pinned_ref = pinned_owned.as_ref();
                self.provider.start_execution(info, client, pinned_ref);
                // Immediately poll once to register waker; if already ready, buffer the result
                // into initial_reply for the next poll.
                if let Some(f) = self.provider.executing_future() {
                    match Pin::new(f).poll(cx) {
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
                                    // Buffer next reply to yield on the following poll.
                                    self.parent.initial_reply = Some(out.raw_reply);
                                }
                                Err(e) => {
                                    if matches!(*e.kind, ErrorKind::Command(ref ce) if ce.code == 43 || ce.code == 237)
                                    {
                                        self.parent.exhausted = true;
                                    }
                                }
                            }
                            let exhausted_now = self.parent.exhausted;
                            self.provider
                                .clear_execution(get_more_out.session, exhausted_now);
                        }
                    }
                }
            }
            return Poll::Ready(Some(Ok(RawBatch::new(initial))));
        }

        if self.parent.exhausted {
            return Poll::Ready(None);
        }

        // If a getMore is in flight, poll it.
        let mut ready = None;
        {
            let provider = &mut self.provider;
            if let Some(f) = provider.executing_future() {
                match Pin::new(f).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(g) => ready = Some(g),
                }
            }
        }
        if let Some(get_more_out) = ready {
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
                    let exhausted_now = self.parent.exhausted;
                    self.provider
                        .clear_execution(get_more_out.session, exhausted_now);
                    // Prefetch the next getMore before returning this batch, if applicable.
                    if !self.parent.exhausted {
                        let info = self.parent.info.clone();
                        let client = self.parent.client.clone();
                        let pinned_owned = self
                            .parent
                            .pinned_connection
                            .handle()
                            .map(|c| c.replicate());
                        let pinned_ref = pinned_owned.as_ref();
                        self.provider.start_execution(info, client, pinned_ref);
                        // Immediately poll once to register waker; if already ready, buffer the
                        // result into initial_reply for the next poll.
                        if let Some(f) = self.provider.executing_future() {
                            match Pin::new(f).poll(cx) {
                                Poll::Pending => {}
                                Poll::Ready(get_more_out2) => {
                                    match get_more_out2.result {
                                        Ok(out2) => {
                                            if out2.exhausted {
                                                self.parent.exhausted = true;
                                            }
                                            if out2.id != 0 {
                                                self.parent.info.id = out2.id;
                                            }
                                            self.parent.info.ns = out2.ns;
                                            self.parent.post_batch_resume_token =
                                                out2.post_batch_resume_token;
                                            self.parent.initial_reply = Some(out2.raw_reply);
                                        }
                                        Err(e) => {
                                            if matches!(*e.kind, ErrorKind::Command(ref ce) if ce.code == 43 || ce.code == 237)
                                            {
                                                self.parent.exhausted = true;
                                            }
                                        }
                                    }
                                    let exhausted_now2 = self.parent.exhausted;
                                    self.provider
                                        .clear_execution(get_more_out2.session, exhausted_now2);
                                }
                            }
                        }
                    }
                    return Poll::Ready(Some(Ok(RawBatch::new(out.raw_reply))));
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
        }

        // Start a getMore if needed.
        let info = self.parent.info.clone();
        let client = self.parent.client.clone();
        let pinned_owned = self
            .parent
            .pinned_connection
            .handle()
            .map(|c| c.replicate());
        let pinned_ref = pinned_owned.as_ref();
        self.provider.start_execution(info, client, pinned_ref);
        Poll::Pending
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
