use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    change_stream::{event::ResumeToken, ChangeStreamData, WatchArgs},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, OperationWithDefaults, Retryability},
    options::{ChangeStreamOptions, SelectionCriteria, WriteConcern},
    BoxFuture,
    ClientSession,
};

use super::Aggregate;

pub(crate) struct ChangeStreamAggregate {
    inner: Aggregate,
    args: WatchArgs,
    resume_data: Option<ChangeStreamData>,
}

impl ChangeStreamAggregate {
    pub(crate) fn new(args: &WatchArgs, resume_data: Option<ChangeStreamData>) -> Result<Self> {
        Ok(Self {
            inner: Self::build_inner(args)?,
            args: args.clone(),
            resume_data,
        })
    }

    fn build_inner(args: &WatchArgs) -> Result<Aggregate> {
        let mut bson_options = Document::new();
        append_options(&mut bson_options, args.options.as_ref())?;

        let mut agg_pipeline = vec![doc! { "$changeStream": bson_options }];
        agg_pipeline.extend(args.pipeline.iter().cloned());
        Ok(Aggregate::new(
            args.target.clone(),
            agg_pipeline,
            args.options.as_ref().map(|o| o.aggregate_options()),
        ))
    }
}

impl OperationWithDefaults for ChangeStreamAggregate {
    type O = (CursorSpecification, ChangeStreamData);
    type Command = Document;

    const NAME: &'static str = "aggregate";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        if let Some(data) = &mut self.resume_data {
            let mut new_opts = self.args.options.clone().unwrap_or_default();
            if let Some(token) = data.resume_token.take() {
                if new_opts.start_after.is_some() && !data.document_returned {
                    new_opts.start_after = Some(token);
                    new_opts.start_at_operation_time = None;
                } else {
                    new_opts.resume_after = Some(token);
                    new_opts.start_after = None;
                    new_opts.start_at_operation_time = None;
                }
            } else {
                let saved_time = new_opts
                    .start_at_operation_time
                    .as_ref()
                    .or(data.initial_operation_time.as_ref());
                if saved_time.is_some() && description.max_wire_version.map_or(false, |v| v >= 7) {
                    new_opts.start_at_operation_time = saved_time.cloned();
                }
            }

            self.inner = Self::build_inner(&WatchArgs {
                options: Some(new_opts),
                ..self.args.clone()
            })?;
        }
        self.inner.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        self.inner.extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        description: &'a StreamDescription,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let op_time = response
                .raw_body()
                .get("operationTime")?
                .and_then(bson::RawBsonRef::as_timestamp);
            let spec = self
                .inner
                .handle_response(response, description, session)
                .await?;

            let mut data = ChangeStreamData {
                resume_token: ResumeToken::initial(self.args.options.as_ref(), &spec),
                ..ChangeStreamData::default()
            };
            let has_no_time = |o: &ChangeStreamOptions| {
                o.start_at_operation_time.is_none()
                    && o.resume_after.is_none()
                    && o.start_after.is_none()
            };
            if self.args.options.as_ref().map_or(true, has_no_time)
                && description.max_wire_version.map_or(false, |v| v >= 7)
                && spec.initial_buffer.is_empty()
                && spec.post_batch_resume_token.is_none()
            {
                data.initial_operation_time = op_time;
            }

            Ok((spec, data))
        }
        .boxed()
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
