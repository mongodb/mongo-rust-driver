use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, Bson, Document, RawBsonRef, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::options::DistinctOptions,
    error::Result,
    operation::{to_feature, ResponseHandlingKind},
    options::ClientOptions,
    Collection,
};

use super::{append_options_to_raw_document, ExecutionContext};
use crate::operation::{default_impl, Feature, Operation, OperationDetails, Retryability};

pub(crate) struct Distinct {
    target: Collection<Document>,
    field_name: String,
    query: Document,
    options: Option<DistinctOptions>,
}

impl Distinct {
    pub fn new(
        target: Collection<Document>,
        field_name: String,
        query: Document,
        options: Option<DistinctOptions>,
    ) -> Self {
        Distinct {
            target,
            field_name,
            query,
            options,
        }
    }
}

impl Operation for Distinct {
    type O = Vec<Bson>;

    const NAME: &'static CStr = cstr!("distinct");

    default_impl!(name, handle_error, update_for_retry, pinned_connection);

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
            "key": self.field_name.clone(),
            "query": RawDocumentBuf::try_from(&self.query)?,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        Ok(response
            .get("atClusterTime")?
            .and_then(RawBsonRef::as_timestamp))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: Response = response.body()?;
        Ok(response.values)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Distinct {}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    values: Vec<Bson>,
}
