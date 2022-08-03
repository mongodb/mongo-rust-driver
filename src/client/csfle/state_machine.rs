use std::convert::TryInto;

use bson::{RawDocumentBuf, Document, RawDocument};
use futures_util::{TryStreamExt, stream};
use mongocrypt::ctx::{Ctx, State};
use tokio::io::{AsyncWriteExt, AsyncReadExt};

use crate::client::options::{ServerAddress, TlsOptions};
use crate::cmap::options::StreamOptions;
use crate::runtime::AsyncStream;
use crate::{Client};
use crate::error::{Error, Result};
use crate::operation::{RawOutput, RunCommand};

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into().map_err(|e| Error::internal(format!("could not parse raw document: {}", e)))
}

impl Client {
    pub(crate) async fn run_mongocrypt_ctx(&self, mut ctx: Ctx, db: Option<&str>) -> Result<RawDocumentBuf> {
        let guard = self.inner.csfle.read().await;
        let csfle = match guard.as_ref() {
            Some(csfle) => csfle,
            None => return Err(Error::internal("no csfle state for mongocrypt ctx")),
        };
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
                    let command = ctx.mongo_op()?.to_raw_document_buf();
                    let db = db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoMarkings state"))?;
                    let op = RawOutput(RunCommand::new_raw(
                        db.to_string(),
                        command,
                        None,
                        None,
                    )?);
                    let mongocryptd_client = csfle.mongocryptd_client.as_ref().ok_or_else(|| Error::internal("mongocryptd client not found"))?;
                    let response = mongocryptd_client.execute_operation(op, None).await?;
                    ctx.mongo_feed(response.raw_body())?;
                    ctx.mongo_done()?;
                }
                State::NeedMongoKeys => {
                    let filter = raw_to_doc(ctx.mongo_op()?)?;
                    let kv_ns = &csfle.opts.key_vault_namespace;
                    let kv_client = csfle.aux_clients.key_vault_client.upgrade().ok_or_else(|| Error::internal("key vault client dropped"))?;
                    let kv_coll = kv_client.database(&kv_ns.db).collection::<RawDocumentBuf>(&kv_ns.coll);
                    let mut cursor = kv_coll.find(filter, None).await?;
                    while let Some(result) = cursor.try_next().await? {
                        ctx.mongo_feed(&result)?
                    }
                    ctx.mongo_done()?;
                }    
                State::NeedKms => {
                    let scope = ctx.kms_scope();
                    let mut kms_ctxen: Vec<Result<_>> = vec![];
                    while let Some(kms_ctx) = scope.next_kms_ctx()? {
                        kms_ctxen.push(Ok(kms_ctx));
                    }
                    stream::iter(kms_ctxen).try_for_each_concurrent(None, |mut kms_ctx| async move {
                        let endpoint = kms_ctx.endpoint()?;
                        let addr = ServerAddress::parse(endpoint)?;
                        let mut stream = AsyncStream::connect(StreamOptions::builder()
                            .address(addr)
                            .tls_options(TlsOptions::default())
                            .build()
                        ).await?;
                        stream.write_all(kms_ctx.message()?).await?;
                        let mut buf = vec![];
                        while kms_ctx.bytes_needed() > 0 {
                            let buf_size = kms_ctx.bytes_needed().try_into().map_err(|e| Error::internal(format!("buffer size overflow: {}", e)))?;
                            buf.resize(buf_size, 0);
                            let count = stream.read(&mut buf).await?;
                            kms_ctx.feed(&buf[0..count])?;
                        }
                        Ok(())
                    }).await?;
                }
                State::Ready => result = Some(ctx.finalize()?.to_owned()),
                State::Done => break,
                _ => todo!(),
            }
        }
        match result {
            Some(doc) => Ok(doc),
            None => Err(Error::internal("libmongocrypt terminated without output")),
        }
    }
}