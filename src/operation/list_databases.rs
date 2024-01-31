use bson::RawDocumentBuf;
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::ListDatabasesOptions,
    error::Result,
    operation::{append_options, OperationWithDefaults, Retryability},
    selection_criteria::{ReadPreference, SelectionCriteria},
};

#[derive(Debug)]
pub(crate) struct ListDatabases {
    name_only: bool,
    options: Option<ListDatabasesOptions>,
}

impl ListDatabases {
    pub fn new(name_only: bool, options: Option<ListDatabasesOptions>) -> Self {
        ListDatabases { name_only, options }
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
