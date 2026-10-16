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
        read_retryable,
        to_feature,
        Operation,
        OperationDetails,
        ResponseContext,
    },
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
    read_retryable!();

    fn details(&self) -> OperationDetails {
        OperationDetails::for_read(
            &self.target,
            to_feature!(self.options, selection_criteria),
            to_feature!(self.options, read_concern),
        )
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
        _context: ResponseContext<'a>,
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
