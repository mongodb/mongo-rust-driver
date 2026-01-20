pub(crate) mod change_stream;

use crate::{
    bson::{doc, Bson, Document},
    bson_compat::{cstr, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, OperationTarget, Retryability},
    options::{AggregateOptions, ReadPreference, SelectionCriteria, WriteConcern},
};

use super::{
    ExecutionContext,
    OperationWithDefaults,
    WriteConcernOnlyBody,
    SERVER_4_4_0_WIRE_VERSION,
};

#[derive(Debug)]
pub(crate) struct Aggregate {
    target: OperationTarget,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
}

impl Aggregate {
    pub(crate) fn new(
        target: OperationTarget,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<AggregateOptions>,
    ) -> Self {
        Self {
            target,
            pipeline: pipeline.into_iter().collect(),
            options,
        }
    }
}

// IMPORTANT: If new method implementations are added here, make sure `ChangeStreamAggregate` has
// the equivalent delegations.
impl OperationWithDefaults for Aggregate {
    type O = CursorSpecification;

    const NAME: &'static CStr = cstr!("aggregate");

    const ZERO_COPY: bool = true;

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            crate::bson_compat::cstr_to_str(Self::NAME): target_bson(&self.target),
            "pipeline": bson_util::to_bson_array(&self.pipeline),
            "cursor": {}
        };

        append_options(&mut body, self.options.as_ref())?;

        if self.is_out_or_merge() {
            if let Ok(cursor_doc) = body.get_document_mut("cursor") {
                cursor_doc.remove("batchSize");
            }
        }

        Ok(Command::new_read(
            Self::NAME,
            target_db_name(&self.target),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            (&body).try_into()?,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        super::cursor_get_at_cluster_time(response)
    }

    fn handle_response_cow<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        if self.is_out_or_merge() {
            let wc_error_info = response.body::<WriteConcernOnlyBody>()?;
            wc_error_info.validate()?;
        };

        let description = context.connection.stream_description()?;

        // The comment should only be propagated to getMore calls on 4.4+.
        let comment = if description.max_wire_version.unwrap_or(0) < SERVER_4_4_0_WIRE_VERSION {
            None
        } else {
            self.options.as_ref().and_then(|opts| opts.comment.clone())
        };

        CursorSpecification::new(
            response.into_owned(),
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_await_time),
            comment,
        )
    }

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
            .into()
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        // for aggregates that write, read concern is supported in MongoDB 4.2+.
        true
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn retryability(&self) -> Retryability {
        if self.is_out_or_merge() {
            Retryability::None
        } else {
            Retryability::Read
        }
    }

    fn override_criteria(&self) -> super::OverrideCriteriaFn {
        if !self.is_out_or_merge() {
            return |_, _| None;
        }
        |criteria, topology| {
            if criteria == &SelectionCriteria::ReadPreference(ReadPreference::Primary)
                || topology.topology_type() == crate::TopologyType::LoadBalanced
            {
                return None;
            }
            for server in topology.servers.values() {
                if let Ok(Some(v)) = server.max_wire_version() {
                    if v < super::SERVER_5_0_0_WIRE_VERSION {
                        return Some(SelectionCriteria::ReadPreference(ReadPreference::Primary));
                    }
                }
            }
            None
        }
    }

    fn target(&self) -> OperationTarget {
        self.target.clone()
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

impl Aggregate {
    /// Returns whether this is a $out or $merge aggregation operation.
    fn is_out_or_merge(&self) -> bool {
        self.pipeline
            .last()
            .map(|stage| {
                let stage = bson_util::first_key(stage);
                stage == Some("$out") || stage == Some("$merge")
            })
            .unwrap_or(false)
    }
}

fn target_bson(target: &OperationTarget) -> Bson {
    match target {
        OperationTarget::Database(_) => Bson::Int32(1),
        OperationTarget::Collection(coll) => Bson::String(coll.name().to_owned()),
        OperationTarget::Disowned(ns) => Bson::String(ns.coll.to_owned()),
        OperationTarget::Null => panic!("invalid aggregate target"),
    }
}

fn target_db_name(target: &OperationTarget) -> &str {
    match target {
        OperationTarget::Database(db) => db.name(),
        OperationTarget::Collection(coll) => coll.db().name(),
        OperationTarget::Disowned(ns) => &ns.db,
        OperationTarget::Null => panic!("invalid aggregate target"),
    }
}
