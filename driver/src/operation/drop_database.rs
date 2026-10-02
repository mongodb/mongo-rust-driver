use crate::{
    bson::rawdoc,
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::DropDatabaseOptions,
    error::Result,
    operation::{
        append_options_to_raw_document,
        default_impl,
        to_feature,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
        Retryability,
    },
    options::ClientOptions,
    Database,
};

#[derive(Debug)]
pub(crate) struct DropDatabase {
    target_db: Database,
    options: Option<DropDatabaseOptions>,
}

impl DropDatabase {
    pub(crate) fn new(target_db: Database, options: Option<DropDatabaseOptions>) -> Self {
        Self { target_db, options }
    }
}

impl Operation for DropDatabase {
    type O = ();

    const NAME: &'static CStr = cstr!("dropDatabase");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, _options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: to_feature!(self.options, write_concern),
            supports_sessions: true,
            retryability: Retryability::None,
            is_backpressure_retryable: false,
            override_criteria: None,
            target: (&self.target_db).into(),
            is_after_cluster_time_write: true,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropDatabase {}
