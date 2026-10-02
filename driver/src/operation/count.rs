use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::options::EstimatedDocumentCountOptions,
    error::{Error, Result},
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
    Collection,
};

pub(crate) struct Count {
    target: Collection<Document>,
    options: Option<EstimatedDocumentCountOptions>,
}

impl Count {
    pub fn new(
        target: Collection<Document>,
        options: Option<EstimatedDocumentCountOptions>,
    ) -> Self {
        Count { target, options }
    }
}

impl Operation for Count {
    type O = u64;

    const NAME: &'static CStr = cstr!("count");

    default_impl!(
        name,
        extract_at_cluster_time,
        update_for_retry,
        pinned_connection
    );

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            selection_criteria: to_feature!(self.options, selection_criteria),
            read_concern: to_feature!(self.options, read_concern),
            write_concern: Feature::NotSupported,
            supports_sessions: true,
            retryability: Retryability::read(options),
            is_backpressure_retryable: options.retry_reads != Some(false),
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
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
        let response_body: ResponseBody = response.body()?;
        Ok(response_body.n)
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(0)
        } else {
            Err(error)
        }
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Count {}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseBody {
    n: u64,
}
