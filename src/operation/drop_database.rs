use crate::bson::rawdoc;

use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::DropDatabaseOptions,
    error::Result,
    operation::{
        append_options_to_raw_document,
        remove_empty_write_concern,
        OperationWithDefaults,
        WriteConcernOnlyBody,
    },
    options::WriteConcern,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct DropDatabase {
    target_db: String,
    options: Option<DropDatabaseOptions>,
}

impl DropDatabase {
    pub(crate) fn new(target_db: String, options: Option<DropDatabaseOptions>) -> Self {
        Self { target_db, options }
    }
}

impl OperationWithDefaults for DropDatabase {
    type O = ();

    const NAME: &'static str = "dropDatabase";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };

        remove_empty_write_concern!(self.options);
        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target_db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
