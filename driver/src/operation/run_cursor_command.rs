use crate::{
    bson_compat::{cstr, CStr},
    cmap::RawCommandResponse,
    cursor::common::CursorSpecification,
    error::Result,
    operation::{
        default_impl,
        forward_impl,
        run_command::RunCommand,
        ExecutionContext,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
    },
    options::{ClientOptions, RunCursorCommandOptions},
};

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
    const NAME: &'static CStr = cstr!("$runCursorCommand");

    forward_impl!(
        run_command,
        name,
        build,
        extract_at_cluster_time,
        update_for_retry,
        pinned_connection
    );

    default_impl!(handle_error);

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Owned,
            ..self.run_command.details(options)
        }
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        CursorSpecification::new(
            response,
            context
                .connection
                .stream_description()?
                .server_address
                .clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_time),
            self.options.as_ref().and_then(|opts| opts.comment.clone()),
        )
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfo for RunCursorCommand<'_> {
    fn log_name(&self) -> &str {
        self.run_command.otel().log_name()
    }

    fn cursor_id(&self) -> Option<i64> {
        self.run_command.otel().cursor_id()
    }

    fn output_cursor_id(output: &<Self as Operation>::O) -> Option<i64> {
        Some(output.id())
    }
}
