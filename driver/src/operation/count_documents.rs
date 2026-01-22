use std::convert::TryInto;

use serde::Deserialize;

use crate::{
    bson::{doc, Document, RawDocument},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, ErrorKind, Result},
    operation::{aggregate::Aggregate, Operation},
    options::{AggregateOptions, CountOptions, SelectionCriteria},
};

use super::{ExecutionContext, Retryability, SingleCursorResult};

pub(crate) struct CountDocuments {
    aggregate: Aggregate,
}

impl CountDocuments {
    pub(crate) fn new(
        coll: &crate::Collection<Document>,
        filter: Document,
        options: Option<CountOptions>,
    ) -> Result<Self> {
        let mut pipeline = vec![doc! {
            "$match": filter,
        }];

        if let Some(skip) = options.as_ref().and_then(|opts| opts.skip) {
            let s: i64 = skip.try_into().map_err(|_| {
                Error::from(ErrorKind::InvalidArgument {
                    message: format!("skip exceeds range of i64: {skip}"),
                })
            })?;
            pipeline.push(doc! {
                "$skip": s
            });
        }

        if let Some(limit) = options.as_ref().and_then(|opts| opts.limit) {
            let l: i64 = limit.try_into().map_err(|_| {
                Error::from(ErrorKind::InvalidArgument {
                    message: format!("limit exceeds range of i64: {limit}"),
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
            aggregate: Aggregate::new(coll.into(), pipeline, aggregate_options),
        })
    }
}

impl Operation for CountDocuments {
    type O = u64;

    const NAME: &'static crate::bson_compat::CStr = Aggregate::NAME;

    const ZERO_COPY: bool = false;

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        self.aggregate.build(description)
    }

    fn extract_at_cluster_time(
        &self,
        response: &RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        self.aggregate.extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        _context: ExecutionContext<'a>,
    ) -> crate::BoxFuture<'a, Result<Self::O>> {
        use futures_util::FutureExt;
        async move {
            let response: SingleCursorResult<Body> = response.body()?;
            Ok(response.0.map(|r| r.n).unwrap_or(0))
        }
        .boxed()
    }

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        self.aggregate.selection_criteria().into()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }

    fn target(&self) -> super::OperationTarget {
        self.aggregate.target()
    }

    fn read_concern(&self) -> super::Feature<&crate::options::ReadConcern> {
        self.aggregate.read_concern()
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }

    fn is_acknowledged(&self) -> bool {
        self.aggregate.is_acknowledged()
    }

    fn write_concern(&self) -> Option<&crate::options::WriteConcern> {
        self.aggregate.write_concern()
    }

    fn supports_sessions(&self) -> bool {
        self.aggregate.supports_sessions()
    }

    fn update_for_retry(&mut self) {
        self.aggregate.update_for_retry();
    }

    fn override_criteria(&self) -> super::OverrideCriteriaFn {
        self.aggregate.override_criteria()
    }

    fn pinned_connection(&self) -> Option<&crate::cmap::conn::PinnedConnectionHandle> {
        self.aggregate.pinned_connection()
    }

    fn name(&self) -> &crate::bson_compat::CStr {
        self.aggregate.name()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for CountDocuments {}

#[derive(Debug, Deserialize)]
struct Body {
    n: u64,
}
