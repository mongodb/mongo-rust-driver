use futures_util::FutureExt;

use crate::{
    bson_compat::CStr,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    options::SelectionCriteria,
    BoxFuture,
};

use super::{ExecutionContext, Operation};

/// Forwards all implementation to the wrapped `Operation`, but returns the response unparsed and
/// unvalidated as a `RawCommandResponse`.
#[derive(Clone)]
pub(crate) struct RawOutput<Op>(pub(crate) Op);

impl<Op: Operation> Operation for RawOutput<Op> {
    type O = RawCommandResponse;
    const NAME: &'static CStr = Op::NAME;
    const ZERO_COPY: bool = true;

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.0.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        self.0.extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        _context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move { Ok(response.into_owned()) }.boxed()
    }

    fn handle_error(&self, error: crate::error::Error) -> Result<Self::O> {
        Err(error)
    }

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        self.0.selection_criteria()
    }

    fn is_acknowledged(&self) -> bool {
        self.0.is_acknowledged()
    }

    fn read_concern(&self) -> super::Feature<&crate::options::ReadConcern> {
        self.0.read_concern()
    }

    fn write_concern(&self) -> Option<&crate::options::WriteConcern> {
        self.0.write_concern()
    }

    fn supports_read_concern(&self) -> bool {
        self.0.supports_read_concern()
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

    fn override_criteria(&self) -> super::OverrideCriteriaFn {
        self.0.override_criteria()
    }

    fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
        self.0.pinned_connection()
    }

    fn name(&self) -> &CStr {
        self.0.name()
    }

    fn target(&self) -> super::OperationTarget {
        self.0.target()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl<Op: Operation> crate::otel::OtelInfo for RawOutput<Op> {
    fn log_name(&self) -> &str {
        self.0.otel().log_name()
    }

    fn cursor_id(&self) -> Option<i64> {
        self.0.otel().cursor_id()
    }

    fn output_cursor_id(_output: &<Self as Operation>::O) -> Option<i64> {
        None
    }
}
