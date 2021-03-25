#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, CommandResponse, StreamDescription},
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
    const NAME: &'static str = "listCollections";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
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
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let body: CursorBody = response.body()?;

        Ok(CursorSpecification::new(
            body.cursor.ns,
            response.source_address().clone(),
            body.cursor.id,
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
            body.cursor.first_batch,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
