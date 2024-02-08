use bson::doc;
use futures_util::TryStreamExt;

use crate::{
    action::{action_impl, DropCollection},
    coll::DropCollectionOptions,
    error::Result,
    operation::drop_collection as op,
    ClientSession,
    Collection,
};

action_impl! {
    impl<'a, T> Action for DropCollection<'a, T> {
        type Future = DropCollectionFuture;

        async fn execute(mut self) -> Result<()> {
            resolve_options!(self.coll, self.options, [write_concern]);

            #[cfg(feature = "in-use-encryption-unstable")]
            self.coll.drop_aux_collections(self.options.as_ref(), self.session.as_deref_mut())
                .await?;

            let drop = op::DropCollection::new(self.coll.namespace(), self.options);
            self.coll.client()
                .execute_operation(drop, self.session.as_deref_mut())
                .await
        }
    }
}

#[cfg(feature = "in-use-encryption-unstable")]
impl<T> Collection<T> {
    #[allow(clippy::needless_option_as_deref)]
    async fn drop_aux_collections(
        &self,
        options: Option<&DropCollectionOptions>,
        mut session: Option<&mut ClientSession>,
    ) -> Result<()> {
        // Find associated `encrypted_fields`:
        // * from options to this call
        let mut enc_fields = options.and_then(|o| o.encrypted_fields.as_ref());
        let enc_opts = self.client().auto_encryption_opts().await;
        // * from client-wide `encrypted_fields_map`:
        let client_enc_fields = enc_opts
            .as_ref()
            .and_then(|eo| eo.encrypted_fields_map.as_ref());
        if enc_fields.is_none() {
            enc_fields =
                client_enc_fields.and_then(|efm| efm.get(&format!("{}", self.namespace())));
        }
        // * from a `list_collections` call:
        let found;
        if enc_fields.is_none() && client_enc_fields.is_some() {
            let filter = doc! { "name": self.name() };
            let mut specs: Vec<_> = match session.as_deref_mut() {
                Some(s) => {
                    let mut cursor = self
                        .inner
                        .db
                        .list_collections()
                        .filter(filter)
                        .session(&mut *s)
                        .await?;
                    cursor.stream(s).try_collect().await?
                }
                None => {
                    self.inner
                        .db
                        .list_collections()
                        .filter(filter)
                        .await?
                        .try_collect()
                        .await?
                }
            };
            if let Some(spec) = specs.pop() {
                if let Some(enc) = spec.options.encrypted_fields {
                    found = enc;
                    enc_fields = Some(&found);
                }
            }
        }

        // Drop the collections.
        if let Some(enc_fields) = enc_fields {
            for ns in crate::client::csfle::aux_collections(&self.namespace(), enc_fields)? {
                let drop = op::DropCollection::new(ns, options.cloned());
                self.client()
                    .execute_operation(drop, session.as_deref_mut())
                    .await?;
            }
        }
        Ok(())
    }
}
