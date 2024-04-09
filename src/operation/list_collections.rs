use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, OperationWithDefaults, Retryability},
    options::{ListCollectionsOptions, ReadPreference, SelectionCriteria},
    BoxFuture,
    ClientSession,
};

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
    type Command = Document;

    const NAME: &'static str = "listCollections";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(filter) = self.options.as_ref().and_then(|o| o.filter.as_ref()) {
            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.insert("nameOnly", name_only);

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME.to_string(), self.db.clone(), body))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        description: &'a StreamDescription,
        _session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let response: CursorBody = response.body()?;
            Ok(CursorSpecification::new(
                response.cursor,
                description.server_address.clone(),
                self.options.as_ref().and_then(|opts| opts.batch_size),
                None,
                None,
            ))
        }
        .boxed()
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
