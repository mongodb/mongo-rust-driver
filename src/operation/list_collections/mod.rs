#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, Operation, Retryability},
    options::{ListCollectionsOptions, ReadPreference, SelectionCriteria},
};

#[derive(Debug)]
pub(crate) struct ListCollections {
    db: String,
    filter: Option<Document>,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
}

impl ListCollections {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(String::new(), None, false, None)
    }

    pub(crate) fn new(
        db: String,
        filter: Option<Document>,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            filter,
            name_only,
            options,
        }
    }
}

impl Operation for ListCollections {
    type O = CursorSpecification;
    type Command = Document;

    const NAME: &'static str = "listCollections";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(ref filter) = self.filter {
            body.insert("filter", filter.clone());

            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.insert("nameOnly", name_only);

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME.to_string(), self.db.clone(), body))
    }

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: CursorBody = raw_response.body()?;
        Ok(CursorSpecification::new(
            response.cursor,
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
        )?)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
