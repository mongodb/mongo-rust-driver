use bson::{Document, doc};
use serde::Deserialize;

use crate::{SearchIndexModel, Namespace, error::Result, cmap::Command};

use super::OperationWithDefaults;

#[derive(Debug)]
pub(crate) struct CreateSearchIndexes {
    ns: Namespace,
    indexes: Vec<SearchIndexModel>,
}

impl CreateSearchIndexes {
    pub(crate) fn new(ns: Namespace, indexes: Vec<SearchIndexModel>) -> Self {
        Self { ns, indexes }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateSearchIndexesResponse {
    indexes_created: Vec<CreatedSearchIndex>,
}

#[derive(Debug, Deserialize)]
struct CreatedSearchIndex {
    #[allow(unused)]
    id: String,
    name: String,
}

impl OperationWithDefaults for CreateSearchIndexes {
    type O = Vec<String>;
    type Command = Document;
    const NAME: &'static str = "createSearchIndexes";

    fn build(&mut self, _description: &crate::cmap::StreamDescription) -> Result<Command> {
        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            doc! {
                Self::NAME: self.ns.coll.clone(),
                "indexes": bson::to_bson(&self.indexes)?,
            }
        ))
    }

    fn handle_response(
        &self,
        response: crate::cmap::RawCommandResponse,
        _description: &crate::cmap::StreamDescription,
    ) -> Result<Self::O> {
        let response: CreateSearchIndexesResponse = response.body()?;
        Ok(response.indexes_created.into_iter().map(|ci| ci.name).collect())
    }
}