use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{
        append_options_to_raw_document,
        default_impl,
        to_feature,
        Operation,
        OperationDetails,
        ResponseContext,
    },
    options::{ClientOptions, DropIndexOptions},
    Collection,
};

pub(crate) struct DropIndexes {
    target: Collection<Document>,
    name: String,
    options: Option<DropIndexOptions>,
}

impl DropIndexes {
    pub(crate) fn new(
        target: Collection<Document>,
        name: String,
        options: Option<DropIndexOptions>,
    ) -> Self {
        Self {
            target,
            name,
            options,
        }
    }
}

impl Operation for DropIndexes {
    type O = ();
    const NAME: &'static CStr = cstr!("dropIndexes");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        retryability,
        pinned_connection
    );

    fn is_backpressure_retryable(&self, options: &ClientOptions) -> bool {
        options.retry_writes != Some(false)
    }

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
            "index": self.name.clone(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_op_details(op_details, self.name(), body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ResponseContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropIndexes {}
