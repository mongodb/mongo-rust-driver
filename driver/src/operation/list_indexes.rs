use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    checked::Checked,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::common::CursorSpecification,
    error::Result,
    operation::{
        append_options_to_raw_document,
        default_impl,
        read_retryable,
        Feature,
        Operation,
        OperationDetails,
        ResponseContext,
    },
    options::ListIndexesOptions,
    selection_criteria::SelectionCriteria,
    Collection,
};

pub(crate) struct ListIndexes {
    target: Collection<Document>,
    options: Option<ListIndexesOptions>,
}

impl ListIndexes {
    pub(crate) fn new(target: Collection<Document>, options: Option<ListIndexesOptions>) -> Self {
        ListIndexes { target, options }
    }
}

impl Operation for ListIndexes {
    type O = CursorSpecification;

    const NAME: &'static CStr = cstr!("listIndexes");

    default_impl!(
        name,
        extract_at_cluster_time,
        handle_error,
        update_for_retry,
        pinned_connection
    );
    read_retryable!();

    fn details(&self) -> OperationDetails {
        OperationDetails::for_read(
            &self.target,
            Feature::Set(SelectionCriteria::primary()),
            Feature::NotSupported,
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
        if let Some(size) = self.options.as_ref().and_then(|o| o.batch_size) {
            let size = Checked::from(size).try_into::<i32>()?;
            body.append(cstr!("cursor"), rawdoc! { "batchSize": size });
        }
        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_op_details(op_details, self.name(), body))
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ResponseContext<'a>,
    ) -> Result<Self::O> {
        CursorSpecification::new(
            response,
            context
                .connection
                .stream_description()?
                .server_address
                .clone(),
            self.options.as_ref().and_then(|o| o.batch_size),
            self.options.as_ref().and_then(|o| o.max_time),
            None,
        )
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for ListIndexes {
    fn output_cursor_id(output: &Self::O) -> Option<i64> {
        Some(output.id())
    }
}
