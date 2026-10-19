use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::prelude::*,
    options::CreateCollectionOptions,
    Collection,
};

#[derive(Debug)]
pub(crate) struct Create {
    target: Collection<Document>,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    pub(crate) fn new(
        target: Collection<Document>,
        options: Option<CreateCollectionOptions>,
    ) -> Self {
        Self { target, options }
    }
}

impl Operation for Create {
    type O = ();

    const NAME: &'static CStr = cstr!("create");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
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
impl crate::otel::OtelInfoDefaults for Create {
    fn log_name(&self) -> &str {
        "createCollection"
    }
}
