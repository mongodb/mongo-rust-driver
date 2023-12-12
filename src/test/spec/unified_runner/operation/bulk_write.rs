use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::{
    bson::{Array, Bson, Document},
    client::bulk_write::{models::WriteModel, BulkWriteOptions},
    coll::options::UpdateModifications,
    error::Result,
    test::spec::unified_runner::{Entity, TestRunner},
    Namespace,
};

use super::TestOperation;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct BulkWrite {
    requests: Vec<WriteModel>,
    #[serde(flatten)]
    options: BulkWriteOptions,
}

impl<'de> Deserialize<'de> for WriteModel {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        enum WriteModelHelper {
            InsertOne {
                namespace: Namespace,
                document: Document,
            },
            UpdateOne {
                namespace: Namespace,
                filter: Document,
                update: UpdateModifications,
                array_filters: Option<Array>,
                collation: Option<Document>,
                hint: Option<Bson>,
                upsert: Option<bool>,
            },
            UpdateMany {
                namespace: Namespace,
                filter: Document,
                update: UpdateModifications,
                array_filters: Option<Array>,
                collation: Option<Document>,
                hint: Option<Bson>,
                upsert: Option<bool>,
            },
            ReplaceOne {
                namespace: Namespace,
                filter: Document,
                replacement: Document,
                array_filters: Option<Array>,
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
                array_filters,
                collation,
                hint,
                upsert,
            } => WriteModel::ReplaceOne {
                namespace,
                filter,
                replacement,
                array_filters,
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
            let mut action = client.bulk_write(self.requests.clone());
            if let Some(ordered) = self.options.ordered {
                action = action.ordered(ordered);
            }
            if let Some(bypass_document_validation) = self.options.bypass_document_validation {
                action = action.bypass_document_validation(bypass_document_validation);
            }
            if let Some(ref comment) = self.options.comment {
                action = action.comment(comment.clone());
            }
            if let Some(ref let_vars) = self.options.let_vars {
                action = action.let_vars(let_vars.clone());
            }
            let bson = if let Some(true) = self.options.verbose_results {
                let result = action.verbose_results().await?;
                bson::to_bson(&result)
            } else {
                let result = action.await?;
                bson::to_bson(&result)
            }?;
            Ok(Some(bson.into()))
        }
        .boxed()
    }
}
