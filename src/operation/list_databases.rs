#[cfg(test)]
mod test;

use bson::RawDocumentBuf;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, OperationWithDefaults, Retryability},
    options::ListDatabasesOptions,
    selection_criteria::{ReadPreference, SelectionCriteria},
};

#[derive(Debug)]
pub(crate) struct ListDatabases {
    filter: Option<Document>,
    name_only: bool,
    options: Option<ListDatabasesOptions>,
}

impl ListDatabases {
    pub fn new(
        filter: Option<Document>,
        name_only: bool,
        options: Option<ListDatabasesOptions>,
    ) -> Self {
        ListDatabases {
            filter,
            name_only,
            options,
        }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        ListDatabases {
            filter: None,
            name_only: false,
            options: None,
        }
    }
}

impl OperationWithDefaults for ListDatabases {
    type O = Vec<RawDocumentBuf>;
    type Command = Document;

    const NAME: &'static str = "listDatabases";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body: Document = doc! {
            Self::NAME: 1,
            "nameOnly": self.name_only
        };

        if let Some(ref filter) = self.filter {
            body.insert("filter", filter.clone());
        }

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            "admin".to_string(),
            body,
        ))
    }

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: Response = raw_response.body()?;
        Ok(response.databases)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    databases: Vec<RawDocumentBuf>,
}
