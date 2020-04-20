#[cfg(test)]
mod test;

use bson::{doc, Document};
use serde::Deserialize;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation},
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

impl Operation for ListDatabases {
    type O = Vec<Document>;
    const NAME: &'static str = "listDatabases";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
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

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        response.body::<ResponseBody>().map(|body| body.databases)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }
}

#[derive(Debug, Deserialize)]
struct ResponseBody {
    databases: Vec<Document>,
    total_size: Option<i64>,
}
