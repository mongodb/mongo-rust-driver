use std::convert::TryInto;
use std::sync::Arc;
use std::sync::mpsc as sync_mpsc;
use std::thread;

use bson::{RawDocumentBuf, Document, RawDocument};
use mongocrypt::ctx::{Ctx, State, CtxBuilder};
use tokio::sync::mpsc::UnboundedSender;

use crate::{Client};
use crate::error::{Error, Result};
use crate::operation::{RawOutput, RunCommand};

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into().map_err(|e| Error::internal(format!("could not parse raw document: {}", e)))
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
                    let _ = send.send(CtxRequest::Err(e));
                    return;
                }
            };
            let _ = match ctx_loop(ctx, send.clone()) {
                Ok(Some(doc)) => send.send(CtxRequest::Done(doc)),
                Ok(None) => send.send(CtxRequest::Err(Error::internal("libmongocrypt terminated without output"))),
                Err(e) => send.send(CtxRequest::Err(e)),
            };
        });
        let thread_err = || Error::internal("ctx thread unexpectedly terminated");
        loop {
            let request = recv.recv().await.ok_or_else(thread_err)?;
            match request {
                CtxRequest::NeedMongoCollinfo { filter, reply } => {
                    let db = self.database(db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoCollinfo state"))?);
                    let mut cursor = db.list_collections(filter, None).await?;
                    if cursor.advance().await? {
                        reply.send(Some(cursor.current().to_raw_document_buf()))
                    } else {
                        reply.send(None)
                    }.map_err(|_| thread_err())?;
                }
                CtxRequest::NeedMongoMarkings { command, reply } => {
                    let db = db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoMarkings state"))?;
                    let op = RawOutput(RunCommand::new_raw(
                        db.to_string(),
                        command,
                        None,
                        None,
                    )?);
                    let guard = self.inner.csfle.read().await;
                    let csfle = guard.as_ref().ok_or_else(|| Error::internal("csfle state not found"))?;
                    let mongocryptd_client = csfle.mongocryptd_client.as_ref().ok_or_else(|| Error::internal("mongocryptd client not found"))?;
                    let result = mongocryptd_client.execute_operation(op, None).await?;
                    reply.send(result.into_raw_document_buf()).map_err(|_| thread_err())?;
                }
                CtxRequest::Done(doc) => return Ok(doc),
                CtxRequest::Err(e) => return Err(e),
            }
        }
    }
}

fn ctx_loop(mut ctx: Ctx, send: UnboundedSender<CtxRequest>) -> Result<Option<RawDocumentBuf>> {
    let mut result = None;
    loop {
        match ctx.state()? {
            State::NeedMongoCollinfo => {
                let filter = raw_to_doc(ctx.mongo_op()?)?;
                let (reply, reply_recv) = sync_oneshot();
                if let Err(_) = send.send(CtxRequest::NeedMongoCollinfo { filter, reply }) {
                    return Ok(None);
                }
                match reply_recv.recv() {
                    Ok(Some(v)) => ctx.mongo_feed(&v)?,
                    Ok(None) => (),
                    Err(_) => return Ok(None),  // waiting async task terminated, no need to keep running
                }
                ctx.mongo_done()?;
            }
            State::NeedMongoMarkings => {
                let command = ctx.mongo_op()?.to_raw_document_buf();
                let (reply, reply_recv) = sync_oneshot();
                if let Err(_) = send.send(CtxRequest::NeedMongoMarkings { command, reply }) {
                    return Ok(None);
                }
                match reply_recv.recv() {
                    Ok(v) => ctx.mongo_feed(&v)?,
                    Err(_) => return Ok(None),
                }
                ctx.mongo_done()?;
            }
            State::Ready => result = Some(ctx.finalize()?.to_owned()),
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
    NeedMongoMarkings {
        command: RawDocumentBuf,
        reply: SyncOneshotSender<RawDocumentBuf>,
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