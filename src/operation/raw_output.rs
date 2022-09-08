use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
};

use super::Operation;

/// Forwards all implementation to the wrapped `Operation`, but returns the response unparsed and
/// unvalidated as a `RawCommandResponse`.
#[derive(Clone)]
pub(crate) struct RawOutput<Op>(pub(crate) Op);

impl<Op: Operation> Operation for RawOutput<Op> {
    type O = RawCommandResponse;
    type Command = Op::Command;
    const NAME: &'static str = Op::NAME;

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        self.0.build(description)
    }

    fn serialize_command(&mut self, cmd: Command<Self::Command>) -> Result<Vec<u8>> {
        self.0.serialize_command(cmd)
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        self.0.extract_at_cluster_time(response)
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(response)
    }

    fn handle_error(&self, error: crate::error::Error) -> Result<Self::O> {
        Err(error)
    }

    fn selection_criteria(&self) -> Option<&crate::selection_criteria::SelectionCriteria> {
        self.0.selection_criteria()
    }

    fn is_acknowledged(&self) -> bool {
        self.0.is_acknowledged()
    }

    fn write_concern(&self) -> Option<&crate::options::WriteConcern> {
        self.0.write_concern()
    }

    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.0.supports_read_concern(description)
    }

    fn supports_sessions(&self) -> bool {
        self.0.supports_sessions()
    }

    fn retryability(&self) -> super::Retryability {
        self.0.retryability()
    }

    fn update_for_retry(&mut self) {
        self.0.update_for_retry()
    }

    fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
        self.0.pinned_connection()
    }

    fn name(&self) -> &str {
        self.0.name()
    }
}
