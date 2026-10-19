use crate::{
    bson::{rawdoc, Document},
    bson_compat::{cstr, CStr},
    bson_util::to_raw_bson_array_ser,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    index::IndexModel,
    operation::prelude::*,
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
        retryability,
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
        self.indexes.iter_mut().for_each(|i| i.update_name()); // Generate names for unnamed indexes.
        let indexes = to_raw_bson_array_ser(&self.indexes)?;
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
            "indexes": indexes,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::from_op_details(op_details, self.name(), body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ResponseContext<'a>,
    ) -> Result<Self::O> {
        response.validate_single_write()?;
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        Ok(CreateIndexesResult { index_names })
    }

    fn is_backpressure_retryable(&self, options: &ClientOptions) -> bool {
        options.retry_writes != Some(false)
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CreateIndexes {}
