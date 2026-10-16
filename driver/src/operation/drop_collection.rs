use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{
        append_options_to_raw_document,
        default_impl,
        to_feature,
        Operation,
        OperationDetails,
        ResponseContext,
    },
    options::DropCollectionOptions,
    Collection,
};

#[derive(Debug)]
pub(crate) struct DropCollection {
    target: Collection<Document>,
    options: Option<DropCollectionOptions>,
}

impl DropCollection {
    pub(crate) fn new(
        target: Collection<Document>,
        options: Option<DropCollectionOptions>,
    ) -> Self {
        DropCollection { target, options }
    }
}

impl Operation for DropCollection {
    type O = ();

    const NAME: &'static CStr = cstr!("drop");

    default_impl!(
        name,
        extract_at_cluster_time,
        update_for_retry,
        retryability,
        is_backpressure_retryable,
        pinned_connection
    );

    fn details(&self) -> OperationDetails {
        OperationDetails::for_write(&self.target, to_feature!(self.options, write_concern))
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
        response.validate_single_write()
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(())
        } else {
            Err(error)
        }
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropCollection {
    fn log_name(&self) -> &str {
        "dropCollection"
    }
}
