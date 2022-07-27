use std::convert::TryInto;
use std::sync::Arc;
use std::sync::mpsc as sync_mpsc;
use std::thread;

use bson::{RawDocumentBuf, Document, RawDocument};
use futures_core::future::{BoxFuture, LocalBoxFuture};
use mongocrypt::ctx::{Ctx, State, CtxBuilder};
use tokio::sync::mpsc::UnboundedSender;

use crate::{Client, Database};
use crate::error::{Error, Result};
use crate::operation::{ListCollections, RawOutput, RunCommand};

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into().map_err(|e| Error::internal("???"))
}

impl Client {
    pub(crate) async fn run_mongocrypt_ctx(&self, build_ctx: impl FnOnce(CtxBuilder) -> Result<Ctx> + Send + 'static, db: Option<&str>) -> Result<RawDocumentBuf> {
        let guard = self.inner.csfle.read().await;
        let crypt = match guard.as_ref() {
            Some(csfle) => Arc::clone(&csfle.crypt),
            None => return Err(Error::internal("no csfle state for mongocrypt ctx")),
        };
        let (send, mut recv) = tokio::sync::mpsc::unbounded_channel();
        thread::spawn(move || {
            let builder = crypt.ctx_builder();
            let ctx = match build_ctx(builder) {
                Ok(c) => c,
                Err(e) => {
                    send.send(CtxRequest::Err(e));
                    return;
                }
            };
            match ctx_loop(ctx, send.clone()) {
                Ok(Some(doc)) => send.send(CtxRequest::Done(doc)),
                Ok(None) => send.send(CtxRequest::Err(Error::internal("libmongocrypt terminated without output"))),
                Err(e) => send.send(CtxRequest::Err(e)),
            };
        });
        fn thread_err() -> Error {
            Error::internal("ctx thread unexpectedly terminated")
        }
        loop {
            let request = recv.recv().await.ok_or_else(thread_err)?;
            match request {
                CtxRequest::NeedMongoCollinfo { filter, reply } => {
                    let db = self.database(db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoCollinfo state"))?);
                    let mut cursor = db.list_collections(filter, None).await?;
                    if cursor.advance().await? {
                        reply.send(Some(cursor.current().to_raw_document_buf()));
                    } else {
                        reply.send(None);
                    }
                }
                CtxRequest::Done(doc) => return Ok(doc),
                CtxRequest::Err(e) => return Err(e),
            }
        }
        /*
        Box::pin(async move {
            let mut result = None;
            loop {
                let state = ctx.state()?;
                match state {
                    State::NeedMongoCollinfo => {
                        let filter = raw_to_doc(ctx.mongo_op()?)?;
                        let db = self.database(db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoCollinfo state"))?);
                        let mut cursor = db.list_collections(filter, None).await?;
                        if cursor.advance().await? {
                            ctx.mongo_feed(cursor.current())?;
                        }
                        ctx.mongo_done()?;
                    }
                    State::NeedMongoMarkings => {
                        let db = db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoMarkings state"))?;
                        let op = RawOutput(RunCommand::new_raw(
                            db.to_string(),
                            ctx.mongo_op()?.to_raw_document_buf(),
                            None,
                            None,
                        )?);
                        let guard = self.inner.csfle.read().await;
                        let csfle = guard.as_ref().ok_or_else(|| Error::internal("csfle state not found"))?;
                        let mongocryptd_client = csfle.mongocryptd_client.as_ref().ok_or_else(|| Error::internal("mongocryptd client not found"))?;
                        let result = mongocryptd_client.execute_operation(op, None).await?;
                        ctx.mongo_feed(result.raw_body())?;
                        ctx.mongo_done()?;
                    }
                    State::Ready => result = Some(ctx.finalize()?.to_owned()),
                    State::Done => break,
                    _ => todo!(),
                }
            }
            result.ok_or_else(|| Error::internal("libmongocrypt terminated without output"))
        })
        */
    }
}

fn ctx_loop(mut ctx: Ctx, send: UnboundedSender<CtxRequest>) -> Result<Option<RawDocumentBuf>> {
    let mut result = None;
    loop {
        match ctx.state()? {
            State::NeedMongoCollinfo => {
                let filter = raw_to_doc(ctx.mongo_op()?)?;
                let (reply, reply_recv) = sync_oneshot();
                // TODO: terminate loop if send fails
                send.send(CtxRequest::NeedMongoCollinfo { filter, reply });
                match reply_recv.recv() {
                    Ok(Some(v)) => ctx.mongo_feed(&v)?,
                    Ok(None) => (),
                    Err(_) => return Ok(None),  // waiting async task terminated, no need to keep running
                }
                ctx.mongo_done()?;
            }
            State::Done => break,
            _ => todo!(),
        }
    }
    Ok(result)
}

enum CtxRequest {
    NeedMongoCollinfo {
        filter: Document,
        reply: SyncOneshotSender<Option<RawDocumentBuf>>,
    },
    Done(RawDocumentBuf),
    Err(Error),
}

struct SyncOneshotSender<T>(sync_mpsc::SyncSender<T>);

impl<T> SyncOneshotSender<T> {
    fn send(self, value: T) -> std::result::Result<(), T> {
        self.0.send(value).map_err(|sync_mpsc::SendError(value)| value)
    }
}

struct SyncOneshotReceiver<T>(sync_mpsc::Receiver<T>);

impl<T> SyncOneshotReceiver<T> {
    fn recv(self) -> std::result::Result<T, sync_mpsc::RecvError> {
        self.0.recv()
    }
}

fn sync_oneshot<T>() -> (SyncOneshotSender<T>, SyncOneshotReceiver<T>) {
    let (sender, receiver) = sync_mpsc::sync_channel(1);
    (SyncOneshotSender(sender), SyncOneshotReceiver(receiver))
}