use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::{
    bson::{to_bson, Array, Bson, Document},
    coll::options::UpdateModifications,
    error::Result,
    options::{BulkWriteOptions, WriteModel},
    test::spec::unified_runner::{Entity, TestRunner},
    Namespace,
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
            InsertOne {
                namespace: Namespace,
                document: Document,
            },
            #[serde(rename_all = "camelCase")]
            UpdateOne {
                namespace: Namespace,
                filter: Document,
                update: UpdateModifications,
                array_filters: Option<Array>,
                collation: Option<Document>,
                hint: Option<Bson>,
                upsert: Option<bool>,
            },
            #[serde(rename_all = "camelCase")]
            UpdateMany {
                namespace: Namespace,
                filter: Document,
                update: UpdateModifications,
                array_filters: Option<Array>,
                collation: Option<Document>,
                hint: Option<Bson>,
                upsert: Option<bool>,
            },
            #[serde(rename_all = "camelCase")]
            ReplaceOne {
                namespace: Namespace,
                filter: Document,
                replacement: Document,
                collation: Option<Document>,
                hint: Option<Bson>,
                upsert: Option<bool>,
            },
            DeleteOne {
                namespace: Namespace,
                filter: Document,
                collation: Option<Document>,
                hint: Option<Bson>,
            },
            DeleteMany {
                namespace: Namespace,
                filter: Document,
                collation: Option<Document>,
                hint: Option<Bson>,
            },
        }

        let helper = WriteModelHelper::deserialize(deserializer)?;
        let model = match helper {
            WriteModelHelper::InsertOne {
                namespace,
                document,
            } => WriteModel::InsertOne {
                namespace,
                document,
            },
            WriteModelHelper::UpdateOne {
                namespace,
                filter,
                update,
                array_filters,
                collation,
                hint,
                upsert,
            } => WriteModel::UpdateOne {
                namespace,
                filter,
                update,
                array_filters,
                collation,
                hint,
                upsert,
            },
            WriteModelHelper::UpdateMany {
                namespace,
                filter,
                update,
                array_filters,
                collation,
                hint,
                upsert,
            } => WriteModel::UpdateMany {
                namespace,
                filter,
                update,
                array_filters,
                collation,
                hint,
                upsert,
            },
            WriteModelHelper::ReplaceOne {
                namespace,
                filter,
                replacement,
                collation,
                hint,
                upsert,
            } => WriteModel::ReplaceOne {
                namespace,
                filter,
                replacement,
                collation,
                hint,
                upsert,
            },
            WriteModelHelper::DeleteOne {
                namespace,
                filter,
                collation,
                hint,
            } => WriteModel::DeleteOne {
                namespace,
                filter,
                collation,
                hint,
            },
            WriteModelHelper::DeleteMany {
                namespace,
                filter,
                collation,
                hint,
            } => WriteModel::DeleteMany {
                namespace,
                filter,
                collation,
                hint,
            },
        };

        Ok(model)
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
