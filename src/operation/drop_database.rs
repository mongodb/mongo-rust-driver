use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::DropDatabaseOptions,
    error::Result,
    operation::{
        append_options,
        remove_empty_write_concern,
        OperationWithDefaults,
        WriteConcernOnlyBody,
    },
    options::WriteConcern,
    BoxFuture,
    ClientSession,
};

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
    type Command = Document;

    const NAME: &'static str = "dropDatabase";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target_db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
        _session: Option<&mut ClientSession>,
    ) -> BoxFuture<'static, Result<Self::O>> {
        async move {
            let response: WriteConcernOnlyBody = response.body()?;
            response.validate()
        }
        .boxed()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
