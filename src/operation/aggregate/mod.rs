#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Bson, Document},
    bson_util,
    cmap::{Command, CommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, Operation, Retryability, WriteConcernOnlyBody},
    options::{AggregateOptions, SelectionCriteria, WriteConcern},
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Aggregate {
    target: AggregateTarget,
    pipeline: Vec<Document>,
    options: Option<AggregateOptions>,
}

impl Aggregate {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(Namespace::empty(), Vec::new(), None)
    }

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

impl Operation for Aggregate {
    type O = CursorSpecification;
    const NAME: &'static str = "aggregate";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.target.to_bson(),
            "pipeline": bson_util::to_bson_array(&self.pipeline),
            "cursor": {}
        };
        append_options(&mut body, self.options.as_ref())?;

        if self.is_out_or_merge() {
            if let Ok(cursor_doc) = body.get_document_mut("cursor") {
                cursor_doc.remove("batchSize");
            }
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target.db_name().to_string(),
            body,
        ))
    }

    fn handle_response(
        &self,
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let body: CursorBody = response.body()?;

        if self.is_out_or_merge() {
            let error_body: WriteConcernOnlyBody = response.body()?;
            error_body.validate()?;
        }

        Ok(CursorSpecification::new(
            body.cursor.ns,
            response.source_address().clone(),
            body.cursor.id,
            self.options.as_ref().and_then(|opts| opts.batch_size),
            self.options.as_ref().and_then(|opts| opts.max_await_time),
            body.cursor.first_batch,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|opts| opts.selection_criteria.as_ref())
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
