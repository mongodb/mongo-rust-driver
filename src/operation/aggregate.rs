pub(crate) mod change_stream;

use crate::{
    bson::{doc, Bson, Document},
    bson_compat::{cstr, CStr},
    bson_util,
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, Retryability},
    options::{AggregateOptions, ReadPreference, SelectionCriteria, WriteConcern},
    Namespace,
};

use super::{
    CursorBody,
    ExecutionContext,
    OperationWithDefaults,
    WriteConcernOnlyBody,
    SERVER_4_4_0_WIRE_VERSION,
};

#[derive(Debug)]
pub(crate) struct Aggregate {
    target: AggregateTarget,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
}

impl Aggregate {
    pub(crate) fn new(
        target: impl Into<AggregateTarget>,
        pipeline: impl IntoIterator<Item = Document>,
        options: Option<AggregateOptions>,
    ) -> Self {
        Self {
            target: target.into(),
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

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            crate::bson_compat::cstr_to_str(Self::NAME): self.target.to_bson(),
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
            self.target.db_name(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            (&body).try_into()?,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        CursorBody::extract_at_cluster_time(response)
    }

    fn handle_response<'a>(
        &'a self,
        response: &RawCommandResponse,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let cursor_response: CursorBody = response.body()?;

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

        Ok(CursorSpecification::new(
            cursor_response.cursor,
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_await_time),
            comment,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
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

#[derive(Clone, Debug)]
pub(crate) enum AggregateTarget {
    Database(String),
    Collection(Namespace),
}

impl AggregateTarget {
    fn to_bson(&self) -> Bson {
        match self {
            AggregateTarget::Database(_) => Bson::Int32(1),
            AggregateTarget::Collection(ref ns) => Bson::String(ns.coll.to_string()),
        }
    }

    fn db_name(&self) -> &str {
        match self {
            AggregateTarget::Database(ref s) => s.as_str(),
            AggregateTarget::Collection(ref ns) => ns.db.as_str(),
        }
    }
}

impl From<Namespace> for AggregateTarget {
    fn from(ns: Namespace) -> Self {
        AggregateTarget::Collection(ns)
    }
}

impl From<String> for AggregateTarget {
    fn from(db_name: String) -> Self {
        AggregateTarget::Database(db_name)
    }
}
