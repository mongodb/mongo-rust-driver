use futures_util::FutureExt;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    BoxFuture,
    ClientSession,
    Namespace,
    SearchIndexModel,
};

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
            },
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> BoxFuture<'static, Result<Self::O>> {
        async move {
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
            Ok(response
                .indexes_created
                .into_iter()
                .map(|ci| ci.name)
                .collect())
        }
        .boxed()
    }

    fn supports_sessions(&self) -> bool {
        false
    }

    fn supports_read_concern(&self, _description: &crate::cmap::StreamDescription) -> bool {
        false
    }
}

#[derive(Debug)]
pub(crate) struct UpdateSearchIndex {
    ns: Namespace,
    name: String,
    definition: Document,
}

impl UpdateSearchIndex {
    pub(crate) fn new(ns: Namespace, name: String, definition: Document) -> Self {
        Self {
            ns,
            name,
            definition,
        }
    }
}

impl OperationWithDefaults for UpdateSearchIndex {
    type O = ();
    type Command = Document;
    const NAME: &'static str = "updateSearchIndex";

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<crate::cmap::Command<Self::Command>> {
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
        _response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> BoxFuture<'static, Result<Self::O>> {
        async move { Ok(()) }.boxed()
    }

    fn supports_sessions(&self) -> bool {
        false
    }

    fn supports_read_concern(&self, _description: &crate::cmap::StreamDescription) -> bool {
        false
    }
}

#[derive(Debug)]
pub(crate) struct DropSearchIndex {
    ns: Namespace,
    name: String,
}

impl DropSearchIndex {
    pub(crate) fn new(ns: Namespace, name: String) -> Self {
        Self { ns, name }
    }
}

impl OperationWithDefaults for DropSearchIndex {
    type O = ();
    type Command = Document;
    const NAME: &'static str = "dropSearchIndex";

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
    ) -> Result<Command<Self::Command>> {
        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            doc! {
                Self::NAME: self.ns.coll.clone(),
                "name": &self.name,
            },
        ))
    }

    fn handle_response(
        &self,
        _response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> BoxFuture<'static, Result<Self::O>> {
        async move { Ok(()) }.boxed()
    }

    fn handle_error(&self, error: crate::error::Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn supports_sessions(&self) -> bool {
        false
    }

    fn supports_read_concern(&self, _description: &crate::cmap::StreamDescription) -> bool {
        false
    }
}
