use futures_util::FutureExt;

use crate::{
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    concern::WriteConcern,
    cursor::CursorSpecification,
    error::{Error, Result},
    operation::{run_command::RunCommand, CursorBody, Operation},
    options::RunCursorCommandOptions,
    selection_criteria::SelectionCriteria,
    BoxFuture,
};

use super::ExecutionContext;

#[derive(Debug, Clone)]
pub(crate) struct RunCursorCommand<'conn> {
    run_command: RunCommand<'conn>,
    options: Option<RunCursorCommandOptions>,
}

impl<'conn> RunCursorCommand<'conn> {
    pub(crate) fn new(
        run_command: RunCommand<'conn>,
        options: Option<RunCursorCommandOptions>,
    ) -> Result<Self> {
        Ok(Self {
            run_command,
            options,
        })
    }
}

impl Operation for RunCursorCommand<'_> {
    type O = CursorSpecification;

    const NAME: &'static str = "run_cursor_command";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.run_command.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        self.run_command.extract_at_cluster_time(response)
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

    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.run_command.supports_read_concern(description)
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

    fn override_criteria(&self) -> super::OverrideCriteriaFn {
        self.run_command.override_criteria()
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.run_command.pinned_connection()
    }

    fn name(&self) -> &str {
        self.run_command.name()
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let cursor_response: CursorBody = response.body()?;

            let comment = match &self.options {
                Some(options) => options.comment.clone(),
                None => None,
            };

            Ok(CursorSpecification::new(
                cursor_response.cursor,
                context
                    .connection
                    .stream_description()?
                    .server_address
                    .clone(),
                self.options.as_ref().and_then(|opts| opts.batch_size),
                self.options.as_ref().and_then(|opts| opts.max_time),
                comment,
            ))
        }
        .boxed()
    }
}
