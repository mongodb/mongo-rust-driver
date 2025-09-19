use crate::bson::{rawdoc, RawDocumentBuf};
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    bson_compat::{cstr, CStr},
    bson_util::to_raw_bson_array_ser,
    cmap::{Command, RawCommandResponse},
    error::Result,
    Namespace,
    SearchIndexModel,
};

use super::{ExecutionContext, OperationWithDefaults};

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
    const NAME: &'static CStr = cstr!("createSearchIndexes");

    fn build(&mut self, _description: &crate::cmap::StreamDescription) -> Result<Command> {
        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            rawdoc! {
                Self::NAME: self.ns.coll.clone(),
                "indexes": to_raw_bson_array_ser(&self.indexes)?,
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
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
        Ok(response
            .indexes_created
            .into_iter()
            .map(|ci| ci.name)
            .collect())
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
    const NAME: &'static CStr = cstr!("updateSearchIndex");

    fn build(
        &mut self,
        _description: &crate::cmap::StreamDescription,
    ) -> crate::error::Result<crate::cmap::Command> {
        let raw_def: RawDocumentBuf = (&self.definition).try_into()?;
        Ok(Command::new(
            Self::NAME,
            &self.ns.db,
            rawdoc! {
                Self::NAME: self.ns.coll.as_str(),
                "name": self.name.as_str(),
                "definition": raw_def,
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
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
    const NAME: &'static CStr = cstr!("dropSearchIndex");

    fn build(&mut self, _description: &crate::cmap::StreamDescription) -> Result<Command> {
        Ok(Command::new(
            Self::NAME,
            &self.ns.db,
            rawdoc! {
                Self::NAME: self.ns.coll.as_str(),
                "name": self.name.as_str(),
            },
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
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
