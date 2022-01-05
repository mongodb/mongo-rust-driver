use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, Operation, Retryability},
    options::{ChangeStreamOptions, SelectionCriteria, WriteConcern}, change_stream::{ChangeStreamData, event::ResumeToken},
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
        let op_time = response.raw_body()
            .get("operationTime")?
            .and_then(bson::RawBsonRef::as_timestamp);
        let mut data = self.data.clone();
        let spec = self.inner.handle_response(response, description)?;

        data.set_resume_token(ResumeToken::initial(data.options(), &spec));
        if self.data.options()
            .map_or(true, |o|
                o.start_at_operation_time.is_none() &&
                o.resume_after.is_none() &&
                o.start_after.is_none()) &&
            description.max_wire_version.map_or(false, |v| v >= 7) &&
            spec.initial_buffer.is_empty() &&
            spec.post_batch_resume_token.is_none() {
                data.set_initial_operation_time(op_time);
        }
    
        Ok((spec, data))
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
