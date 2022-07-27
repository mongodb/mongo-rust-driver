use std::convert::TryInto;

use bson::{RawDocumentBuf, Document, RawDocument};
use futures_core::future::{BoxFuture, LocalBoxFuture};
use mongocrypt::ctx::{Ctx, State};

use crate::{Client, Database};
use crate::error::{Error, Result};
use crate::operation::{ListCollections, RawOutput, RunCommand};

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into().map_err(|e| Error::internal("???"))
}

impl Client {
    pub(crate) fn run_mongocrypt_ctx<'a>(&'a self, mut ctx: Ctx, db: Option<&'a str>) -> LocalBoxFuture<'a, Result<RawDocumentBuf>> {
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
    }
}