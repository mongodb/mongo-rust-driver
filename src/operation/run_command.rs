#[cfg(test)]
mod test;

use std::convert::TryInto;

use bson::{RawBsonRef, RawDocumentBuf};

use super::{CursorBody, OperationWithDefaults};
use crate::{
    bson::Document,
    client::SESSIONS_UNSUPPORTED_COMMANDS,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    selection_criteria::SelectionCriteria,
};

#[derive(Debug, Clone)]
pub(crate) struct RunCommand<'conn> {
    db: String,
    command: RawDocumentBuf,
    selection_criteria: Option<SelectionCriteria>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
}

impl<'conn> RunCommand<'conn> {
    pub(crate) fn new(
        db: String,
        command: Document,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Result<Self> {
        Ok(Self {
            db,
            command: RawDocumentBuf::from_document(&command)?,
            selection_criteria,
            pinned_connection,
        })
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) fn new_raw(
        db: String,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Result<Self> {
        Ok(Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
        })
    }

    fn command_name(&self) -> Option<&str> {
        self.command
            .into_iter()
            .next()
            .and_then(|r| r.ok())
            .map(|(k, _)| k)
    }
}

impl<'conn> OperationWithDefaults for RunCommand<'conn> {
    type O = Document;
    type Command = RawDocumentBuf;

    // Since we can't actually specify a string statically here, we just put a descriptive string
    // that should fail loudly if accidentally passed to the server.
    const NAME: &'static str = "$genericRunCommand";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command<Self::Command>> {
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
