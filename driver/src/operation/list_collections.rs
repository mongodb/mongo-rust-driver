use crate::{
    bson::rawdoc,
    bson_compat::{cstr, CStr},
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
    options::{ListCollectionsOptions, SelectionCriteria},
    Database,
};

#[derive(Debug)]
pub(crate) struct ListCollections {
    db: Database,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
}

impl ListCollections {
    pub(crate) fn new(
        db: Database,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            name_only,
            options,
        }
    }
}

impl Operation for ListCollections {
    type O = CursorSpecification;

    const NAME: &'static CStr = cstr!("listCollections");

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
            &self.db,
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
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(filter) = self.options.as_ref().and_then(|o| o.filter.as_ref()) {
            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.append(cstr!("nameOnly"), name_only);

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            body,
        ))
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
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
            None,
        )
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for ListCollections {
    fn output_cursor_id(output: &Self::O) -> Option<i64> {
        Some(output.id())
    }
}
