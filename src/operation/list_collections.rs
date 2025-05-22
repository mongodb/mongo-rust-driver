use crate::bson::rawdoc;

use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{CursorBody, OperationWithDefaults, Retryability},
    options::{ListCollectionsOptions, ReadPreference, SelectionCriteria},
};

use super::{append_options_to_raw_document, ExecutionContext};

#[derive(Debug)]
pub(crate) struct ListCollections {
    db: String,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
}

impl ListCollections {
    pub(crate) fn new(
        db: String,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            name_only,
            options,
        }
    }
}

impl OperationWithDefaults for ListCollections {
    type O = CursorSpecification;

    const NAME: &'static str = "listCollections";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(filter) = self.options.as_ref().and_then(|o| o.filter.as_ref()) {
            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.append("nameOnly", name_only);

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME.to_string(), self.db.clone(), body))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: CursorBody = response.body()?;
        Ok(CursorSpecification::new(
            response.cursor,
            context
                .connection
                .stream_description()?
                .server_address
                .clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
            None,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
