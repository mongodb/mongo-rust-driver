use bson::RawDocumentBuf;
use mongocrypt::ctx::{Ctx, State};

use crate::Client;
use crate::error::Result;

impl Client {
    pub(crate) async fn run_mongocrypt_ctx(&self, ctx: &mut Ctx) -> Result<RawDocumentBuf> {
        let mut result = RawDocumentBuf::new();
        loop {
            match ctx.state()? {
                State::NeedMongoCollinfo => {
                    let filter = ctx.mongo_op()?;
                }
                _ => todo!(),
            }
        }
        Ok(result)
    }
}