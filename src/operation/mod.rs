use crate::{
    cmap::conn::{
        command::{Command, CommandResponse},
        StreamDescription,
    },
    error::Result,
    sdam::SelectionCriteria,
};

/// A trait modeling the behavior of a server side operation.
pub(super) trait Operation {
    /// The output type of this operation.
    type O;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    fn build(&self, description: &StreamDescription) -> Result<Command>;

    /// Interprets the server response to the command.
    fn handle_response(&self, response: CommandResponse) -> Result<Self::O>;

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        None
    }
}
