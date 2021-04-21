#[cfg(test)]
mod test;

use bson::{doc, Document};

use super::{Operation, Retryability};
use crate::{
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    operation::aggregate::Aggregate,
    options::{AggregateOptions, CountOptions},
    selection_criteria::SelectionCriteria,
    Namespace,
};

pub(crate) struct CountDocuments {
    aggregate: Aggregate,
}

impl CountDocuments {
    pub(crate) fn new(
        namespace: Namespace,
        filter: Option<Document>,
        options: Option<CountOptions>,
    ) -> Self {
        let mut pipeline = vec![doc! {
            "$match": filter.unwrap_or_default(),
        }];

        if let Some(skip) = options.as_ref().and_then(|opts| opts.skip) {
            pipeline.push(doc! {
                "$skip": skip
            });
        }

        if let Some(limit) = options.as_ref().and_then(|opts| opts.limit) {
            pipeline.push(doc! {
                "$limit": limit
            });
        }

        pipeline.push(doc! {
            "$group": {
                "_id": 1,
                "n": { "$sum": 1 },
            }
        });

        let aggregate_options = options.map(|opts| {
            let mut agg_options = AggregateOptions::builder().build();
            agg_options.hint = opts.hint;
            agg_options.max_time = opts.max_time;
            agg_options.collation = opts.collation;
            agg_options.selection_criteria = opts.selection_criteria;
            agg_options.read_concern = opts.read_concern;
            agg_options
        });

        Self {
            aggregate: Aggregate::new(namespace, pipeline, aggregate_options),
        }
    }
}

impl Operation for CountDocuments {
    type O = i64;
    const NAME: &'static str = Aggregate::NAME;

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        self.aggregate.build(description)
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let result = self
            .aggregate
            .handle_response(response, description)
            .map(|mut spec| spec.initial_buffer.pop_front())?;

        let result_doc = match result {
            Some(doc) => doc,
            None => return Ok(0),
        };

        let n = match result_doc.get("n") {
            Some(n) => n,
            None => {
                return Err(ErrorKind::ResponseError {
                    message: "server response to count_documents aggregate did not contain the \
                              'n' field"
                        .into(),
                }
                .into())
            }
        };

        bson_util::get_int(n).ok_or_else(|| {
            ErrorKind::ResponseError {
                message: format!(
                    "server response to count_documents aggregate should have contained integer \
                     'n', but instead had {:?}",
                    n
                ),
            }
            .into()
        })
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.aggregate.selection_criteria()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
