#[cfg(test)]
mod test;

use bson::RawBsonRef;
use serde::Deserialize;

use crate::{
    bson::{doc, Bson, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::DistinctOptions, Namespace},
    error::Result,
    operation::{append_options, OperationWithDefaults, Retryability},
    selection_criteria::SelectionCriteria,
};

pub(crate) struct Distinct {
    ns: Namespace,
    field_name: String,
    query: Option<Document>,
    options: Option<DistinctOptions>,
}

impl Distinct {
    pub fn new(
        ns: Namespace,
        field_name: String,
        query: Option<Document>,
        options: Option<DistinctOptions>,
    ) -> Self {
        Distinct {
            ns,
            field_name,
            query,
            options,
        }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Distinct {
            ns: Namespace {
                db: String::new(),
                coll: String::new(),
            },
            field_name: String::new(),
            query: None,
            options: None,
        }
    }
}

impl OperationWithDefaults for Distinct {
    type O = Vec<Bson>;
    type Command = Document;

    const NAME: &'static str = "distinct";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body: Document = doc! {
            Self::NAME: self.ns.coll.clone(),
            "key": self.field_name.clone(),
        };

        if let Some(ref query) = self.query {
            body.insert("query", query.clone());
        }

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &bson::RawDocument,
    ) -> Result<Option<bson::Timestamp>> {
        Ok(response
            .get("atClusterTime")?
            .and_then(RawBsonRef::as_timestamp))
    }

    fn handle_response(
        &self,
        response: RawCommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: Response = response.body()?;
        Ok(response.values)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        if let Some(ref options) = self.options {
            return options.selection_criteria.as_ref();
        }
        None
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    values: Vec<Bson>,
}
