use bson::{Binary, Document};
use mongocrypt::ctx::{Algorithm, CtxBuilder};

use crate::{
    action::{
        action_impl,
        csfle::encrypt::{Encrypt, EncryptKey, EncryptOptions, Expression, Value},
    },
    error::{Error, Result},
};

use super::ClientEncryption;

action_impl! {
    impl<'a> Action for Encrypt<'a, Value> {
        type Future = EncryptFuture;

        async fn execute(self) -> Result<Binary> {
            let ctx = self
                .client_enc
                .get_ctx_builder(self.key, self.algorithm, self.options.unwrap_or_default())?
                .build_explicit_encrypt(self.mode.value)?;
            let result = self.client_enc.exec.run_ctx(ctx, None).await?;
            let bin_ref = result
                .get_binary("v")
                .map_err(|e| Error::internal(format!("invalid encryption result: {}", e)))?;
            Ok(bin_ref.to_binary())
        }
    }
}

action_impl! {
    impl<'a> Action for Encrypt<'a, Expression> {
        type Future = EncryptExpressionFuture;

        async fn execute(self) -> Result<Document> {
            let ctx = self
                .client_enc
                .get_ctx_builder(self.key, self.algorithm, self.options.unwrap_or_default())?
                .build_explicit_encrypt_expression(self.mode.value)?;
            let result = self.client_enc.exec.run_ctx(ctx, None).await?;
            let doc_ref = result
                .get_document("v")
                .map_err(|e| Error::internal(format!("invalid encryption result: {}", e)))?;
            let doc = doc_ref
                .to_owned()
                .to_document()
                .map_err(|e| Error::internal(format!("invalid encryption result: {}", e)))?;
            Ok(doc)
        }
    }
}

impl ClientEncryption {
    fn get_ctx_builder(
        &self,
        key: EncryptKey,
        algorithm: Algorithm,
        opts: EncryptOptions,
    ) -> Result<CtxBuilder> {
        let mut builder = self.crypt.ctx_builder();
        match key {
            EncryptKey::Id(id) => {
                builder = builder.key_id(&id.bytes)?;
            }
            EncryptKey::AltName(name) => {
                builder = builder.key_alt_name(&name)?;
            }
        }
        builder = builder.algorithm(algorithm)?;
        if let Some(factor) = opts.contention_factor {
            builder = builder.contention_factor(factor)?;
        }
        if let Some(qtype) = &opts.query_type {
            builder = builder.query_type(qtype)?;
        }
        if let Some(range_options) = &opts.range_options {
            let options_doc = bson::to_document(range_options)?;
            builder = builder.range_options(options_doc)?;
        }
        Ok(builder)
    }
}
