use crate::cmap::{StreamDescription, RawCommandResponse, Command};
use crate::error::Result;

use super::Operation;

pub(crate) struct RawOutput<Op>(Op);

impl<Op: Operation> Operation for RawOutput<Op> {
    type O = RawCommandResponse;
    type Command = Op::Command;
    const NAME: &'static str = Op::NAME;

    fn build(&mut self, description: &StreamDescription) -> Result<Command<Self::Command>> {
        todo!()
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        todo!()
    }

    fn serialize_command(&mut self, cmd: Command<Self::Command>) -> Result<Vec<u8>> {
        Ok(bson::to_vec(&cmd)?)
    }

    fn extract_at_cluster_time(&self, _response: &bson::RawDocument) -> Result<Option<bson::Timestamp>> {
        Ok(None)
    }

    fn handle_error(&self, error: crate::error::Error) -> Result<Self::O> {
        Err(error)
    }

    fn selection_criteria(&self) -> Option<&crate::selection_criteria::SelectionCriteria> {
        None
    }

    fn is_acknowledged(&self) -> bool {
        self.write_concern()
            .map(crate::options::WriteConcern::is_acknowledged)
            .unwrap_or(true)
    }

    fn write_concern(&self) -> Option<&crate::options::WriteConcern> {
        None
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        false
    }

    fn supports_sessions(&self) -> bool {
        true
    }

    fn retryability(&self) -> super::Retryability {
        super::Retryability::None
    }

    fn update_for_retry(&mut self) {}

    fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
        None
    }

    fn name(&self) -> &str {
        Self::NAME
    }
}