use bson::{to_bson, Document};
use serde::Deserialize;
use crate::test::spec::unified_runner::operation::TestOperation;
use crate::options::{ReplaceOptions, UpdateModifications, UpdateOptions};
use crate::test::spec::unified_runner::{Entity, TestRunner};
use futures::future::BoxFuture;
use crate::test::spec::unified_runner::operation::{with_opt_session, with_mut_session};
use crate::error::Result;
use futures_util::FutureExt;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateMany {
    filter: Document,
    update: UpdateModifications,
    session: Option<String>,
    #[serde(flatten)]
    options: UpdateOptions,
}

impl TestOperation for UpdateMany {
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
                    .update_many(self.filter.clone(), self.update.clone())
                    .with_options(self.options.clone()),
            )
            .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: UpdateOptions,
    session: Option<String>,
}

impl TestOperation for UpdateOne {
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
                    .update_one(self.filter.clone(), self.update.clone())
                    .with_options(self.options.clone()),
            )
            .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: ReplaceOptions,
}

impl TestOperation for ReplaceOne {
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
                    .replace_one(self.filter.clone(), self.replacement.clone())
                    .with_options(self.options.clone())
            )
            .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

