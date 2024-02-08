use crate::{
    action::{action_impl, DropCollection},
    error::Result,
    operation::drop_collection as op,
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
