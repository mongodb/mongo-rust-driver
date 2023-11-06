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
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            indexes_created: Vec<CreatedIndex>,
        }

        #[derive(Debug, Deserialize)]
        struct CreatedIndex {
            #[allow(unused)]
            id: String,
            name: String,
        }

        let response: Response = response.body()?;
        Ok(response.indexes_created.into_iter().map(|ci| ci.name).collect())
    }
}

#[derive(Debug)]
pub(crate) struct UpdateSearchIndex {
    ns: Namespace,
    name: String,
    definition: Document,
}

impl UpdateSearchIndex {
    pub(crate) fn new(
        ns: Namespace,
        name: String,
        definition: Document,
    ) -> Self {
        Self { ns, name, definition }
    }
}

impl OperationWithDefaults for UpdateSearchIndex {
    type O = ();
    type Command = Document;
    const NAME: &'static str = "updateSearchIndex";

    fn build(&mut self, _description: &crate::cmap::StreamDescription) -> crate::error::Result<crate::cmap::Command<Self::Command>> {
        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            doc! {
                Self::NAME: self.ns.coll.clone(),
                "name": &self.name,
                "definition": &self.definition,
            },
        ))
    }

    fn handle_response(
        &self,
        response: crate::cmap::RawCommandResponse,
        _description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<Self::O> {
        response.body()
    }
}