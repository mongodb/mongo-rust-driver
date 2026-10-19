use crate::{bson_compat::CStr, cmap::RawCommandResponse, error::Result, operation::prelude::*};

/// Forwards all implementation to the wrapped `Operation`, but returns the response unparsed and
/// unvalidated as a `RawCommandResponse`.
#[derive(Clone)]
pub(crate) struct RawOutput<Op>(pub(crate) Op);

impl<Op: Operation> Operation for RawOutput<Op> {
    type O = RawCommandResponse;

    const NAME: &'static CStr = Op::NAME;

    forward_impl!(
        0,
        name,
        build,
        extract_at_cluster_time,
        update_for_retry,
        retryability,
        is_backpressure_retryable,
        pinned_connection
    );

    default_impl!(handle_error);

    fn details(&self) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Owned,
            ..self.0.details()
        }
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ResponseContext<'a>,
    ) -> Result<Self::O> {
        Ok(response)
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
