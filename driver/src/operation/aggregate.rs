pub(crate) mod change_stream;

use crate::{
    bson::{doc, Bson, Document},
    bson_compat::{cstr, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::common::CursorSpecification,
    error::Result,
    operation::{
        append_options,
        default_impl,
        to_feature,
        ExecutionContext,
        Operation,
        OperationDetails,
        OperationTarget,
        ResponseHandlingKind,
        Retryability,
        SERVER_5_0_0_WIRE_VERSION,
    },
    options::{AggregateOptions, ClientOptions, SelectionCriteria},
    sdam::TopologyDescription,
    TopologyType,
};

#[derive(Debug)]
pub(crate) struct Aggregate {
    target: OperationTarget,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
    is_out_or_merge: bool,
}

impl Aggregate {
    pub(crate) fn new(
        target: OperationTarget,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<AggregateOptions>,
    ) -> Self {
        let pipeline = pipeline.into_iter().collect::<Vec<_>>();
        let is_out_or_merge = pipeline
            .last()
            .map(|stage| {
                let stage = bson_util::first_key(stage);
                stage == Some("$out") || stage == Some("$merge")
            })
            .unwrap_or(false);
        Self {
            target,
            pipeline,
            options,
            is_out_or_merge,
        }
    }
}

impl Operation for Aggregate {
    type O = CursorSpecification;

    const NAME: &'static CStr = cstr!("aggregate");

    default_impl!(name, handle_error, update_for_retry, pinned_connection);

    fn details(&self, options: &ClientOptions) -> OperationDetails {
        let override_criteria = |criteria: &SelectionCriteria, topology: &TopologyDescription| {
            if criteria.is_primary() || topology.topology_type() == TopologyType::LoadBalanced {
                return None;
            }
            if topology.servers.values().any(|server| {
                server
                    .max_wire_version()
                    .ok()
                    .flatten()
                    .is_some_and(|mwv| mwv < SERVER_5_0_0_WIRE_VERSION)
            }) {
                return Some(SelectionCriteria::primary());
            } else {
                return None;
            }
        };

        OperationDetails {
            response_handling_kind: ResponseHandlingKind::Owned,
            selection_criteria: to_feature!(self.options, selection_criteria),
            read_concern: to_feature!(self.options, read_concern),
            write_concern: to_feature!(self.options, write_concern),
            supports_sessions: true,
            retryability: if self.is_out_or_merge {
                Retryability::None
            } else {
                Retryability::read(options)
            },
            is_backpressure_retryable: if self.is_out_or_merge {
                options.retry_writes != Some(false)
            } else {
                options.retry_reads != Some(false)
            },
            override_criteria: self.is_out_or_merge.then_some(override_criteria),
            target: self.target.clone(),
            is_after_cluster_time_write: false,
        }
    }

    fn build(
        &mut self,
        _description: &StreamDescription,
        op_details: &OperationDetails,
    ) -> Result<Command> {
        let mut body = doc! {
            crate::bson_compat::cstr_to_str(Self::NAME): target_bson(&self.target),
            "pipeline": bson_util::to_bson_array(&self.pipeline),
            "cursor": {}
        };

        append_options(&mut body, self.options.as_ref())?;

        if self.is_out_or_merge {
            if let Ok(cursor_doc) = body.get_document_mut("cursor") {
                cursor_doc.remove("batchSize");
            }
        }

        Ok(Command::from_operation_details(
            op_details,
            self.name(),
            (&body).try_into()?,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        super::cursor_get_at_cluster_time(response)
    }

    fn handle_response_owned<'a>(
        &'a self,
        response: RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        if self.is_out_or_merge {
            response.validate_single_write()?;
        };
        CursorSpecification::new(
            response,
            context
                .connection
                .stream_description()?
                .server_address
                .clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_await_time),
            self.options.as_ref().and_then(|opts| opts.comment.clone()),
        )
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Aggregate {
    fn output_cursor_id(output: &Self::O) -> Option<i64> {
        Some(output.id())
    }
}

fn target_bson(target: &OperationTarget) -> Bson {
    match target {
        OperationTarget::Database(_) => Bson::Int32(1),
        OperationTarget::Collection(coll) => Bson::String(coll.name().to_owned()),
        OperationTarget::Namespace(ns) => Bson::String(ns.coll.to_owned()),
    }
}
