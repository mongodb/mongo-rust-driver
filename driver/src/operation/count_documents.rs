use std::convert::TryInto;

use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    bson_compat::CStr,
    cmap::RawCommandResponse,
    error::{Error, ErrorKind, Result},
    operation::{
        aggregate::Aggregate,
        default_impl,
        forward_impl,
        ExecutionContext,
        Operation,
        OperationDetails,
        ResponseHandlingKind,
        Retryability,
        SingleCursorResult,
    },
    options::{AggregateOptions, ClientOptions, CountOptions},
};

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

    const NAME: &'static CStr = Aggregate::NAME;

    forward_impl!(
        aggregate,
        name,
        build,
        extract_at_cluster_time,
        update_for_retry,
        pinned_connection
    );

    default_impl!(handle_error);

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Borrowed,
            retryability: Retryability::read(options),
            is_backpressure_retryable: options.retry_reads != Some(false),
            ..self.aggregate.details(options)
        }
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: SingleCursorResult<Body> = response.body()?;
        Ok(response.0.map(|r| r.n).unwrap_or(0))
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
