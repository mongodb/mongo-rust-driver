#[cfg(test)]
mod test;

use std::convert::TryInto;

use bson::RawBsonRef;

use super::{CursorBody, Operation, OperationWithDefaults};
use crate::{
    bson::Document,
    client::SESSIONS_UNSUPPORTED_COMMANDS,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
};

#[derive(Debug)]
pub(crate) struct RunCommand<'conn> {
    db: String,
    command: Document,
    selection_criteria: Option<SelectionCriteria>,
    write_concern: Option<WriteConcern>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
}

impl<'conn> RunCommand<'conn> {
    pub(crate) fn new(
        db: String,
        command: Document,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Result<Self> {
        let write_concern = command
            .get("writeConcern")
            .map(|doc| bson::from_bson::<WriteConcern>(doc.clone()))
            .transpose()?;

        Ok(Self {
            db,
            command,
            selection_criteria,
            write_concern,
            pinned_connection,
        })
    }

    fn command_name(&self) -> Option<&str> {
        self.command.keys().next().map(String::as_str)
    }
}

impl<'conn> OperationWithDefaults for RunCommand<'conn> {
    type O = Document;
    type Command = Document;

    // Since we can't actually specify a string statically here, we just put a descriptive string
    // that should fail loudly if accidentally passed to the server.
    const NAME: &'static str = "$genericRunCommand";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let command_name = self
            .command_name()
            .ok_or_else(|| ErrorKind::InvalidArgument {
                message: "an empty document cannot be passed to a run_command operation".into(),
            })?;

        Ok(Command::new(
            command_name.to_string(),
            self.db.clone(),
            self.command.clone(),
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        if let Some(RawBsonRef::Timestamp(ts)) = response.get("atClusterTime")? {
            Ok(Some(ts))
        } else {
            CursorBody::extract_at_cluster_time(response)
        }
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(response.into_raw_document_buf().try_into()?)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.selection_criteria.as_ref()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.write_concern.as_ref()
    }

    fn supports_sessions(&self) -> bool {
        self.command_name()
            .map(|command_name| {
                !SESSIONS_UNSUPPORTED_COMMANDS.contains(command_name.to_lowercase().as_str())
            })
            .unwrap_or(false)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }
}
