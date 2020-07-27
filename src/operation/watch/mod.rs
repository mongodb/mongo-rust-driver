#[cfg(test)]
mod test;

use super::aggregate::{Aggregate, AggregateTarget};

use crate::{
    bson::{doc, Document},
    change_stream::{ChangeStreamSpecification, ChangeStreamTarget},
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation},
    options::ChangeStreamOptions,
};

#[derive(Debug)]
pub(crate) struct Watch {
    target: ChangeStreamTarget,
    pipeline: Vec<Document>,
    options: Option<ChangeStreamOptions>,
    aggregate_operation: Aggregate,
}

impl Watch {
    pub(crate) fn new(
        target: impl Into<ChangeStreamTarget>,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<ChangeStreamOptions>,
    ) -> Result<Self> {
        let target: ChangeStreamTarget = target.into();
        let pipeline: Vec<Document> = pipeline.into_iter().collect();

        let mut options_doc: Document = Document::new();
        append_options(&mut options_doc, options.as_ref())?;
        let aggregate_target = match &target {
            ChangeStreamTarget::Collection(namespace) => {
                AggregateTarget::Collection(namespace.clone())
            }
            ChangeStreamTarget::Database(database) => AggregateTarget::Database(database.clone()),
            ChangeStreamTarget::Cluster(database) => {
                options_doc.insert("allChangesForCluster", true);
                AggregateTarget::Database(database.clone())
            }
        };
        let mut aggregate_pipeline = vec![doc! { "$changeStream": options_doc }];
        aggregate_pipeline.extend(pipeline.clone());

        let aggregate_operation = Aggregate::new(
            aggregate_target,
            aggregate_pipeline,
            options.as_ref().map(|opts| opts.get_aggregation_options()),
        );
        Ok(Self {
            target,
            pipeline: pipeline.into_iter().collect(),
            options,
            aggregate_operation,
        })
    }
}

impl Operation for Watch {
    type O = ChangeStreamSpecification;
    const NAME: &'static str = "watch";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        self.aggregate_operation.build(description)
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let cursor_spec = self.aggregate_operation.handle_response(response)?;
        Ok(ChangeStreamSpecification::new(
            cursor_spec,
            self.pipeline.clone(),
            self.options.clone(),
            self.target.clone(),
        ))
    }
}
