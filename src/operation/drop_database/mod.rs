#[cfg(test)]
mod test;

use bson::Document;

use crate::{
    bson::doc,
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{append_options, remove_empty_write_concern, Operation, WriteConcernOnlyBody},
    options::{DropDatabaseOptions, WriteConcern},
};

use super::CommandResponse;

#[derive(Debug)]
pub(crate) struct DropDatabase {
    target_db: String,
    options: Option<DropDatabaseOptions>,
}

impl DropDatabase {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(String::new(), None)
    }

    pub(crate) fn new(target_db: String, options: Option<DropDatabaseOptions>) -> Self {
        Self { target_db, options }
    }
}

impl Operation for DropDatabase {
    type O = ();
    type Command = Document;
    type Response = CommandResponse<WriteConcernOnlyBody>;

    const NAME: &'static str = "dropDatabase";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, Some(&self.options))?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target_db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: WriteConcernOnlyBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
