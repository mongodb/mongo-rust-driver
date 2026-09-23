use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::ListDatabasesOptions,
    error::Result,
    operation::{
        append_options_to_raw_document,
        default_impl,
        ExecutionContext,
        Feature,
        Operation,
        OperationDetails,
        OperationTarget,
        ResponseHandlingKind,
        Retryability,
    },
    options::ClientOptions,
    selection_criteria::SelectionCriteria,
    Client,
};

#[derive(Debug)]
pub(crate) struct ListDatabases {
    client: Client,
    name_only: bool,
    options: Option<ListDatabasesOptions>,
}

impl ListDatabases {
    pub fn new(client: Client, name_only: bool, options: Option<ListDatabasesOptions>) -> Self {
        ListDatabases {
            client,
            name_only,
            options,
        }
    }
}

impl Operation for ListDatabases {
    type O = Vec<RawDocumentBuf>;

    const NAME: &'static CStr = cstr!("listDatabases");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: Feature::Set(SelectionCriteria::primary()),
            read_concern: Feature::NotSupported,
            write_concern: Feature::NotSupported,
            supports_sessions: true,
            retryability: Retryability::read(options),
            is_backpressure_retryable: options.retry_reads != Some(false),
            override_criteria: None,
            target: OperationTarget::admin(&self.client),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
            "nameOnly": self.name_only
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
        let response: Response = response.body()?;
        Ok(response.databases)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for ListDatabases {}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    databases: Vec<RawDocumentBuf>,
}
