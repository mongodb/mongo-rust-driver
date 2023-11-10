#![allow(missing_docs, unused_variables, dead_code)]

pub(crate) mod models;

use serde::{Deserialize, Serialize, Serializer};
use serde_with::skip_serializing_none;

use crate::{
    bson::{Bson, Document},
    error::Result,
    operation::BulkWrite,
    Client,
};

use models::{add_ids_to_insert_one_models, WriteModel};

impl Client {
    pub async fn bulk_write(
        &self,
        models: impl IntoIterator<Item = WriteModel>,
        options: impl Into<Option<BulkWriteOptions>>,
    ) -> Result<()> {
        let mut models: Vec<_> = models.into_iter().collect();
        let inserted_ids = add_ids_to_insert_one_models(&mut models)?;

        let bulk_write = BulkWrite {
            models: models,
            options: options.into(),
            client: self.clone(),
        };
        self.execute_operation(bulk_write, None).await.map(|_| ())
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BulkWriteOptions {
    pub ordered: Option<bool>,
    pub bypass_document_validation: Option<bool>,
    pub comment: Option<Bson>,
    #[serde(rename = "let")]
    pub let_vars: Option<Document>,
    #[serialize_always]
    #[serde(rename = "errorsOnly", serialize_with = "serialize_opposite_bool")]
    pub verbose_results: Option<bool>,
}

fn serialize_opposite_bool<S: Serializer>(
    val: &Option<bool>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let val = !val.unwrap_or(false);
    serializer.serialize_bool(val)
}
