use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    bson_util::to_raw_bson_array_ser,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    index::IndexModel,
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
    options::{ClientOptions, CreateIndexOptions},
    results::CreateIndexesResult,
    Collection,
};

#[derive(Debug)]
pub(crate) struct CreateIndexes {
    target: Collection<Document>,
    indexes: Vec<IndexModel>,
    options: Option<CreateIndexOptions>,
}

impl CreateIndexes {
    pub(crate) fn new(
        target: Collection<Document>,
        indexes: Vec<IndexModel>,
        options: Option<CreateIndexOptions>,
    ) -> Self {
        Self {
            target,
            indexes,
            options,
        }
    }
}

impl Operation for CreateIndexes {
    type O = CreateIndexesResult;
    const NAME: &'static CStr = cstr!("createIndexes");

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
            selection_criteria: Feature::NotSupported,
            read_concern: Feature::NotSupported,
            write_concern: to_feature!(self.options, write_concern),
            supports_sessions: true,
            retryability: Retryability::None,
            is_backpressure_retryable: options.retry_writes != Some(false),
            override_criteria: None,
            target: (&self.target).into(),
            is_after_cluster_time_write: true,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        self.indexes.iter_mut().for_each(|i| i.update_name()); // Generate names for unnamed indexes.
        let indexes = to_raw_bson_array_ser(&self.indexes)?;
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
            "indexes": indexes,
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
        response.validate_single_write()?;
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        Ok(CreateIndexesResult { index_names })
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CreateIndexes {}
