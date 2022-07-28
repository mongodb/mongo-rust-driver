use std::convert::TryInto;
use std::sync::Arc;
use std::sync::mpsc as sync_mpsc;
use std::thread;

use bson::{RawDocumentBuf, Document, RawDocument};
use futures_util::TryStreamExt;
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

        loop {
            let request = recv.recv().await.ok_or_else(thread_err)?;
            match request {
                CtxRequest::NeedMongoCollinfo { filter, reply } => {
                    let db = self.database(db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoCollinfo state"))?);
                    let mut cursor = db.list_collections(filter, None).await?;
                    reply.send(if cursor.advance().await? {
                        Some(cursor.current().to_raw_document_buf())
                    } else {
                        None
                    })?;
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
                    reply.send(result.into_raw_document_buf())?;
                }
                CtxRequest::NeedMongoKeys { filter, reply } => {
                    let guard = self.inner.csfle.read().await;
                    let csfle = guard.as_ref().ok_or_else(|| Error::internal("csfle state not found"))?;
                    let kv_ns = &csfle.opts.key_vault_namespace;
                    let kv_client = csfle.aux_clients.key_vault_client.upgrade().ok_or_else(|| Error::internal("key vault client dropped"))?;
                    let kv_coll = kv_client.database(&kv_ns.db).collection::<RawDocumentBuf>(&kv_ns.coll);
                    let results: Vec<_> = kv_coll.find(filter, None).await?.try_collect().await?;
                    reply.send(results)?;
                }
                CtxRequest::Done(doc) => return Ok(doc),
                CtxRequest::Err(e) => return Err(e),
            }
        }
    }
}

fn thread_err() -> Error {
    Error::internal("ctx thread unexpectedly terminated")
}

fn ctx_err<T>(_: T) -> Error {
    Error::internal("ctx thread could not communicate with async task")
}

fn ctx_loop(mut ctx: Ctx, send: UnboundedSender<CtxRequest>) -> Result<Option<RawDocumentBuf>> {
    let mut result = None;
    loop {
        match ctx.state()? {
            State::NeedMongoCollinfo => {
                let filter = raw_to_doc(ctx.mongo_op()?)?;
                let (reply, reply_recv) = reply_oneshot();
                send.send(CtxRequest::NeedMongoCollinfo { filter, reply }).map_err(ctx_err)?;
                if let Some(v) = reply_recv.recv()? {
                    ctx.mongo_feed(&v)?;
                }
                ctx.mongo_done()?;
            }
            State::NeedMongoMarkings => {
                let command = ctx.mongo_op()?.to_raw_document_buf();
                let (reply, reply_recv) = reply_oneshot();
                send.send(CtxRequest::NeedMongoMarkings { command, reply }).map_err(ctx_err)?;
                ctx.mongo_feed(&reply_recv.recv()?)?;
                ctx.mongo_done()?;
            }
            State::NeedMongoKeys => {
                let filter = raw_to_doc(ctx.mongo_op()?)?;
                let (reply, reply_recv) = reply_oneshot();
                send.send(CtxRequest::NeedMongoKeys { filter, reply }).map_err(ctx_err)?;
                for v in reply_recv.recv()? {
                    ctx.mongo_feed(&v)?;
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
        reply: ReplySender<Option<RawDocumentBuf>>,
    },
    NeedMongoMarkings {
        command: RawDocumentBuf,
        reply: ReplySender<RawDocumentBuf>,
    },
    NeedMongoKeys {
        filter: Document,
        reply: ReplySender<Vec<RawDocumentBuf>>,
    },
    Done(RawDocumentBuf),
    Err(Error),
}

struct ReplySender<T>(sync_mpsc::SyncSender<T>);

impl<T> ReplySender<T> {
    fn send(self, value: T) -> Result<()> {
        self.0.send(value).map_err(|_| thread_err())
    }
}

struct ReplyReceiver<T>(sync_mpsc::Receiver<T>);

impl<T> ReplyReceiver<T> {
    fn recv(self) -> Result<T> {
        self.0.recv().map_err(ctx_err)
    }
}

/// This is a sync version of `tokio::sync::oneshot::channel`; sending will never block (and so can be used in async code), receiving will block until a value is sent.
fn reply_oneshot<T>() -> (ReplySender<T>, ReplyReceiver<T>) {
    let (sender, receiver) = sync_mpsc::sync_channel(1);
    (ReplySender(sender), ReplyReceiver(receiver))
}