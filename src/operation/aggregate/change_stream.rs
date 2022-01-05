use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, Operation, Retryability},
    options::{ChangeStreamOptions, SelectionCriteria, WriteConcern}, change_stream::ChangeStreamData,
};

use super::{Aggregate, AggregateTarget};

pub(crate) struct ChangeStreamAggregate {
    inner: Aggregate,
    data: ChangeStreamData,
}

impl ChangeStreamAggregate {
    pub(crate) fn new(
        target: &AggregateTarget,
        pipeline: &[Document],
        options: &Option<ChangeStreamOptions>,
    ) -> Result<Self> {
        let data = ChangeStreamData::new(pipeline.iter().cloned().collect(), target.clone(), options.clone());

        let mut bson_options = Document::new();
        append_options(&mut bson_options, options.as_ref())?;

        let mut agg_pipeline = vec![doc! { "$changeStream": bson_options }];
        agg_pipeline.extend(pipeline.iter().cloned());
        Ok(Self {
            inner: Aggregate::new(
                target.clone(),
                agg_pipeline,
                options.as_ref().map(|o| o.aggregate_options()),
            ),
            data,
        })
    }
}

impl Operation for ChangeStreamAggregate {
    type O = (CursorSpecification, ChangeStreamData);
    type Command = Document;

    const NAME: &'static str = "aggregate";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.inner.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        self.inner.extract_at_cluster_time(response)
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok((self.inner.handle_response(response, description)?, self.data.clone()))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.inner.selection_criteria()
    }

    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.inner.supports_read_concern(description)
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.inner.write_concern()
    }

    fn retryability(&self) -> Retryability {
        self.inner.retryability()
    }
}
