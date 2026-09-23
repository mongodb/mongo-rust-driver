use std::convert::TryInto;

use crate::{
    bson::{Document, RawBsonRef, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    client::SESSIONS_UNSUPPORTED_COMMANDS,
    cmap::{conn::PinnedConnectionHandle, Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{
        default_impl,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
        Retryability,
    },
    options::ClientOptions,
    selection_criteria::SelectionCriteria,
    Database,
};

#[derive(Debug, Clone)]
pub(crate) struct RunCommand<'conn> {
    db: Database,
    command: RawDocumentBuf,
    selection_criteria: Option<SelectionCriteria>,
    pinned_connection: Option<&'conn PinnedConnectionHandle>,
}

impl<'conn> RunCommand<'conn> {
    pub(crate) fn new(
        db: Database,
        command: RawDocumentBuf,
        selection_criteria: Option<SelectionCriteria>,
        pinned_connection: Option<&'conn PinnedConnectionHandle>,
    ) -> Self {
        Self {
            db,
            command,
            selection_criteria,
            pinned_connection,
        }
    }

    fn command_name(&self) -> Option<&CStr> {
        self.command
            .into_iter()
            .next()
            .and_then(|r| r.ok())
            .map(|(k, _)| k)
    }
}

impl Operation for RunCommand<'_> {
    type O = Document;

    // Since we can't actually specify a string statically here, we just put a descriptive string
    // that should fail loudly if accidentally passed to the server.
    const NAME: &'static CStr = cstr!("$genericRunCommand");

    default_impl!(handle_error, update_for_retry);

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            // Per spec, runCommand MUST ignore any default read preference from client, database
            // or collection configuration
            selection_criteria: match &self.selection_criteria {
                Some(s) => Feature::Set(s.clone()),
                None => Feature::NotSupported,
            },
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: self
                .command_name()
                .map(|command_name| {
                    !SESSIONS_UNSUPPORTED_COMMANDS.contains(command_name.to_lowercase().as_str())
                })
                .unwrap_or(false),
            retryability: Retryability::None,
            is_backpressure_retryable: options.retry_reads != Some(false)
                && options.retry_writes != Some(false),
            override_criteria: None,
            target: (&self.db).into(),
            is_after_cluster_time_write: false,
        }
    }

    fn name(&self) -> &CStr {
        self.command_name().unwrap_or(Self::NAME)
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        if self.command_name().is_none() {
            return Err(Error::invalid_argument(
                "an empty document cannot be passed to a run_command operation",
            ));
        }

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            self.command.clone(),
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        if let Some(RawBsonRef::Timestamp(ts)) = response.get("atClusterTime")? {
            Ok(Some(ts))
        } else {
            super::cursor_get_at_cluster_time(response)
        }
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(response.raw_body().try_into()?)
    }

    fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        self.pinned_connection
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for RunCommand<'_> {}
