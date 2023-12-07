#![allow(missing_docs, unused_variables, dead_code)]

pub(crate) mod actions;
pub(crate) mod models;
pub(crate) mod results;

use serde::{Deserialize, Serialize, Serializer};
use serde_with::skip_serializing_none;

use crate::{
    bson::{Bson, Document},
    Client,
};

use actions::SummaryBulkWriteAction;
use models::WriteModel;

impl Client {
    pub fn bulk_write(
        &self,
        models: impl IntoIterator<Item = WriteModel>,
    ) -> SummaryBulkWriteAction {
        SummaryBulkWriteAction::new(self.clone(), models.into_iter().collect())
    }
}

#[skip_serializing_none]
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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
