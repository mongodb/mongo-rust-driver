use bson::RawDocumentBuf;
use mongocrypt::ctx::Ctx;

use crate::Client;
use crate::error::Result;

impl Client {
    async fn run_mongocrypt_ctx(&self, ctx: &mut Ctx) -> Result<RawDocumentBuf> {
        todo!()
    }
}