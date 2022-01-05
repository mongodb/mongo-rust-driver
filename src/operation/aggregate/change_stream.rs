use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, Operation, Retryability},
    options::{ChangeStreamOptions, SelectionCriteria, WriteConcern}, change_stream::{ChangeStreamData, event::ResumeToken, WatchArgs},
};

use super::{Aggregate};

pub(crate) struct ChangeStreamAggregate {
    inner: Aggregate,
    options: Option<ChangeStreamOptions>,
}

impl ChangeStreamAggregate {
    pub(crate) fn new(
        args: &WatchArgs,
        resume_data: Option<ChangeStreamData>,
    ) -> Result<Self> {
        let mut bson_options = Document::new();
        append_options(&mut bson_options, args.options())?;

        let mut agg_pipeline = vec![doc! { "$changeStream": bson_options }];
        agg_pipeline.extend(args.pipeline().iter().cloned());
        Ok(Self {
            inner: Aggregate::new(
                args.target().clone(),
                agg_pipeline,
                args.options().map(|o| o.aggregate_options()),
            ),
            options: args.options().cloned(),
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
        let spec = self.inner.handle_response(response, description)?;

        let mut data = ChangeStreamData::default();
        data.set_resume_token(ResumeToken::initial(self.options.as_ref(), &spec));
        if self.options
            .as_ref()
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
