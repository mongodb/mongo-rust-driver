use futures_core::future::BoxFuture;
use futures_util::FutureExt;
use serde::Deserialize;

use crate::{
    bson::Document,
    client::bulk_write::models::WriteModel,
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
    verbose_results: Option<bool>,
}

impl<'de> Deserialize<'de> for WriteModel {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as DeError;

        #[derive(Deserialize)]
        struct WriteModelHelper {
            namespace: Namespace,
            document: Option<Document>,
            filter: Option<Document>,
            update: Option<UpdateModifications>,
            replacement: Option<Document>,
        }

        let model_doc = Document::deserialize(deserializer)?;
        let Some((key, value)) = model_doc.into_iter().next() else {
            return Err(DeError::custom("empty write model"));
        };
        let body: WriteModelHelper = bson::from_bson(value).map_err(DeError::custom)?;

        let model = match key.as_str() {
            "insertOne" => WriteModel::InsertOne {
                namespace: body.namespace,
                document: body.document.unwrap(),
            },
            "updateOne" => WriteModel::UpdateOne {
                namespace: body.namespace,
                filter: body.filter.unwrap(),
                update: body.update.unwrap(),
            },
            "updateMany" => WriteModel::UpdateMany {
                namespace: body.namespace,
                filter: body.filter.unwrap(),
                update: body.update.unwrap(),
            },
            "replaceOne" => WriteModel::ReplaceOne {
                namespace: body.namespace,
                filter: body.filter.unwrap(),
                replacement: body.replacement.unwrap(),
            },
            "deleteOne" => WriteModel::DeleteOne {
                namespace: body.namespace,
                filter: body.filter.unwrap(),
            },
            "deleteMany" => WriteModel::DeleteMany {
                namespace: body.namespace,
                filter: body.filter.unwrap(),
            },
            other => {
                return Err(DeError::custom(format!(
                    "unknown bulkWrite operation: {other}"
                )))
            }
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
            let action = client.bulk_write(self.requests.clone());
            let bson = if let Some(true) = self.verbose_results {
                let result = action.verbose_results().await?;
                bson::to_bson(&result)?
            } else {
                let result = action.await?;
                bson::to_bson(&result)?
            };
            Ok(Some(bson.into()))
        }
        .boxed()
    }
}
