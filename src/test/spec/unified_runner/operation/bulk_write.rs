use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::{
    bson::to_bson,
    error::Result,
    options::{
        BulkWriteOptions,
        DeleteManyModel,
        DeleteOneModel,
        InsertOneModel,
        ReplaceOneModel,
        UpdateManyModel,
        UpdateOneModel,
        WriteModel,
    },
    test::spec::unified_runner::{Entity, TestRunner},
};

use super::{with_mut_session, with_opt_session, TestOperation};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BulkWrite {
    session: Option<String>,
    models: Vec<WriteModel>,
    verbose_results: Option<bool>,
    #[serde(flatten)]
    options: BulkWriteOptions,
}

impl<'de> Deserialize<'de> for WriteModel {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        enum WriteModelHelper {
            InsertOne(InsertOneModel),
            UpdateOne(UpdateOneModel),
            UpdateMany(UpdateManyModel),
            ReplaceOne(ReplaceOneModel),
            DeleteOne(DeleteOneModel),
            DeleteMany(DeleteManyModel),
        }

        match WriteModelHelper::deserialize(deserializer)? {
            WriteModelHelper::InsertOne(model) => Ok(Self::InsertOne(model)),
            WriteModelHelper::UpdateOne(model) => Ok(Self::UpdateOne(model)),
            WriteModelHelper::UpdateMany(model) => Ok(Self::UpdateMany(model)),
            WriteModelHelper::ReplaceOne(model) => Ok(Self::ReplaceOne(model)),
            WriteModelHelper::DeleteOne(model) => Ok(Self::DeleteOne(model)),
            WriteModelHelper::DeleteMany(model) => Ok(Self::DeleteMany(model)),
        }
    }
}

impl TestOperation for BulkWrite {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).await;
            let action = client
                .bulk_write(self.models.clone())
                .with_options(self.options.clone());
            let result = if let Some(true) = self.verbose_results {
                with_opt_session!(test_runner, &self.session, action.verbose_results())
                    .await
                    .and_then(|result| Ok(to_bson(&result)?))
            } else {
                with_opt_session!(test_runner, &self.session, action)
                    .await
                    .and_then(|result| Ok(to_bson(&result)?))
            }?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}
