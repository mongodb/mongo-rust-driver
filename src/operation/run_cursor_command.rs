

#[cfg(feature = "in-use-encryption-unstable")]
use bson::doc;
use bson::RawDocumentBuf;


use crate::{
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    concern::{WriteConcern},
    cursor::{CursorSpecification},
    error::{Error, Result},
    operation::{
        Operation,
        RunCommand,
    },
    options::{
        RunCursorCommandOptions,
    },
    selection_criteria::SelectionCriteria,
};

#[derive(Debug, Clone)]
pub(crate) struct RunCursorCommand<'conn> {
    run_command: RunCommand<'conn>,
    options: RunCursorCommandOptions,
}

impl<'conn> RunCursorCommand<'conn>{
    pub(crate) fn new (
        run_command: RunCommand<'conn>,
        options: RunCursorCommandOptions,
    ) -> Result<Self> {
        Ok(Self{
            run_command,
            options,
        })
    }
}

impl<'conn> Operation for RunCursorCommand<'conn> {
    type O = CursorSpecification;
    type Command = RawDocumentBuf;

    const NAME: &'static str = "run_cursor_command";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command<Self::Command>> {
        self.run_command.build(_description)
    }

    fn serialize_command(&mut self, cmd: Command<Self::Command>) -> Result<Vec<u8>> {
        self.run_command.serialize_command(cmd)
    }

    fn extract_at_cluster_time(
        &self,
        _response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        self.run_command.extract_at_cluster_time(_response)
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.run_command.selection_criteria()
    }

    fn is_acknowledged(&self) -> bool {
        self.run_command.is_acknowledged()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.run_command.write_concern()
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        self.run_command.supports_read_concern(_description)
    }

    fn supports_sessions(&self) -> bool {
        self.run_command.supports_sessions()
    }

    fn retryability(&self) -> crate::operation::Retryability {
        self.run_command.retryability()
    }

    fn update_for_retry(&mut self) {
        self.run_command.update_for_retry()
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.run_command.pinned_connection()
    }

    fn name(&self) -> &str {
        self.run_command.name()
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let doc = Operation::handle_response(&self.run_command, response, description)?;
        let cursor_info = bson::from_document(doc)?;
        Ok(CursorSpecification::new(
            cursor_info,
            description.server_address.clone(),
            self.options.batch_size,
            self.options.max_time,
            self.options.comment.clone(),
        ))
    }
}