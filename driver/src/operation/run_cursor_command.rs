use futures_util::FutureExt;

use crate::{
    bson_compat::{cstr, CStr},
    cmap::RawCommandResponse,
    cursor::common::CursorSpecification,
    error::Result,
    operation::{
        run_command::RunCommand,
        Operation,
        OperationImpl,
        OperationWrapper,
        Wrapper,
        SERVER_4_4_0_WIRE_VERSION,
    },
    options::RunCursorCommandOptions,
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

impl<'conn> OperationWrapper for RunCursorCommand<'conn> {
    type Wrapped = RunCommand<'conn>;
    type O = CursorSpecification;
    const NAME: &'static CStr = cstr!("run_cursor_command");
    const ZERO_COPY: bool = true;

    fn wrapped(&self) -> &Self::Wrapped {
        &self.run_command
    }

    fn wrapped_mut(&mut self) -> &mut Self::Wrapped {
        &mut self.run_command
    }

    fn handle_response<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        context: ExecutionContext<'a>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let description = context.connection.stream_description()?;

            // The comment should only be propagated to getMore calls on 4.4+.
            let comment = if description.max_wire_version.unwrap_or(0) < SERVER_4_4_0_WIRE_VERSION {
                None
            } else {
                self.options.as_ref().and_then(|opts| opts.comment.clone())
            };

            CursorSpecification::new(
                response.into_owned(),
                description.server_address.clone(),
                self.options.as_ref().and_then(|opts| opts.batch_size),
                self.options.as_ref().and_then(|opts| opts.max_time),
                comment,
            )
        }
        .boxed()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

impl OperationImpl for RunCursorCommand<'_> {
    type Kind = Wrapper;
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
