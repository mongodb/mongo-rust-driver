use std::convert::TryInto;

use serde::Deserialize;

use super::{OperationWithDefaults, Retryability, SingleCursorResult};
use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, ErrorKind, Result},
    operation::aggregate::Aggregate,
    options::{AggregateOptions, CountOptions},
    selection_criteria::SelectionCriteria,
    Namespace,
};
use bson::{doc, Document, RawDocument};

pub(crate) struct CountDocuments {
    aggregate: Aggregate,
}

impl CountDocuments {
    pub(crate) fn new(
        namespace: Namespace,
        filter: Document,
        options: Option<CountOptions>,
    ) -> Result<Self> {
        let mut pipeline = vec![doc! {
            "$match": filter,
        }];

        if let Some(skip) = options.as_ref().and_then(|opts| opts.skip) {
            let s: i64 = skip.try_into().map_err(|_| {
                Error::from(ErrorKind::InvalidArgument {
                    message: format!("skip exceeds range of i64: {}", skip),
                })
            })?;
            pipeline.push(doc! {
                "$skip": s
            });
        }

        if let Some(limit) = options.as_ref().and_then(|opts| opts.limit) {
            let l: i64 = limit.try_into().map_err(|_| {
                Error::from(ErrorKind::InvalidArgument {
                    message: format!("limit exceeds range of i64: {}", limit),
                })
            })?;
            pipeline.push(doc! {
                "$limit": l
            });
        }

        pipeline.push(doc! {
            "$group": {
                "_id": 1,
                "n": { "$sum": 1 },
            }
        });

        let aggregate_options = options.map(|opts| {
            AggregateOptions::builder()
                .hint(opts.hint)
                .max_time(opts.max_time)
                .collation(opts.collation)
                .selection_criteria(opts.selection_criteria)
                .read_concern(opts.read_concern)
                .comment(opts.comment)
                .build()
        });

        Ok(Self {
            aggregate: Aggregate::new(namespace, pipeline, aggregate_options),
        })
    }
}

impl OperationWithDefaults for CountDocuments {
    type O = u64;
    type Command = Document;

    const NAME: &'static str = Aggregate::NAME;

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.aggregate.build(description)
    }

    fn extract_at_cluster_time(&self, response: &RawDocument) -> Result<Option<bson::Timestamp>> {
        self.aggregate.extract_at_cluster_time(response)
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: SingleCursorResult<Body> = response.body()?;
        Ok(response.0.map(|r| r.n).unwrap_or(0))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.aggregate.selection_criteria()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }

    fn supports_read_concern(&self, description: &StreamDescription) -> bool {
        self.aggregate.supports_read_concern(description)
    }
}

#[derive(Debug, Deserialize)]
struct Body {
    n: u64,
}
