use crate::{
    bson::Document,
    bson_compat::serialize_to_bson,
    error::Result,
    options::DeleteOptions,
    test::spec::unified_runner::{
        operation::{with_mut_session, with_opt_session, TestOperation},
        Entity,
        TestRunner,
    },
};
use futures::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteMany {
    filter: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: DeleteOptions,
}

impl TestOperation for DeleteMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                collection
                    .delete_many(self.filter.clone())
                    .with_options(self.options.clone())
            )
            .await?;
            let result = serialize_to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteOne {
    filter: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: DeleteOptions,
}

impl TestOperation for DeleteOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = with_opt_session!(
                test_runner,
                &self.session,
                collection
                    .delete_one(self.filter.clone())
                    .with_options(self.options.clone()),
            )
            .await?;
            let result = serialize_to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}
