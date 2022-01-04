use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, Operation, Retryability},
    options::{ChangeStreamOptions, SelectionCriteria, WriteConcern},
};

use super::{Aggregate, AggregateTarget};

pub(crate) struct ChangeStreamAggregate(Aggregate);

impl ChangeStreamAggregate {
    pub(crate) fn new(
        target: &AggregateTarget,
        pipeline: &[Document],
        options: &Option<ChangeStreamOptions>,
    ) -> Result<Self> {
        let mut bson_options = Document::new();
        append_options(&mut bson_options, options.as_ref())?;

        let mut agg_pipeline = vec![doc! { "$changeStream": bson_options }];
        agg_pipeline.extend(pipeline.iter().cloned());
        Ok(Self(Aggregate::new(
            target.clone(),
            agg_pipeline,
            options.as_ref().map(|o| o.aggregate_options()),
        )))
    }
}

impl Operation for ChangeStreamAggregate {
    type O = CursorSpecification;
    type Command = Document;

    const NAME: &'static str = "aggregate";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.0.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        self.0.extract_at_cluster_time(response)
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        self.0.handle_response(response, description)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.0.selection_criteria()
    }

    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.0.supports_read_concern(description)
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.0.write_concern()
    }

    fn retryability(&self) -> Retryability {
        self.0.retryability()
    }
}
