use std::convert::TryInto;

use bson::{RawDocumentBuf, Document, RawDocument};
use mongocrypt::ctx::{Ctx, State};

use crate::{Client, Database};
use crate::error::{Error, Result};
use crate::operation::{ListCollections, RawOutput, RunCommand};

fn raw_to_doc(raw: &RawDocument) -> Result<Document> {
    raw.try_into().map_err(|e| Error::internal("???"))
}

impl Client {
    pub(crate) async fn run_mongocrypt_ctx(&self, ctx: &mut Ctx, db: Option<Database>) -> Result<RawDocumentBuf> {
        let mut result = RawDocumentBuf::new();
        loop {
            match ctx.state()? {
                State::NeedMongoCollinfo => {
                    let filter = raw_to_doc(ctx.mongo_op()?)?;
                    let db = db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoCollinfo state"))?;
                    let mut cursor = db.list_collections(filter, None).await?;
                    if cursor.advance().await? {
                        ctx.mongo_feed(cursor.current())?;
                    }
                    ctx.mongo_done()?;
                }
                State::NeedMongoMarkings => {
                    let db = db.as_ref().ok_or_else(|| Error::internal("db required for NeedMongoMarkings state"))?;
                    let op = RawOutput(RunCommand::new_raw(
                        db.name().to_string(),
                        ctx.mongo_op()?.to_raw_document_buf(),
                        None,
                        None,
                    )?);
                    let result = self.execute_operation(op, None).await?;
                    ctx.mongo_feed(result.raw_body())?;
                    ctx.mongo_done()?;
                }
                State::Ready => result = ctx.finalize()?.to_owned(),
                State::Done => break,    
                _ => todo!(),
            }
        }
        Ok(result)
    }
}