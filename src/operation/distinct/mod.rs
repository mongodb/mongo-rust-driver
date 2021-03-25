#[cfg(test)]
mod test;

use serde::Deserialize;

use crate::{
    bson::{doc, Bson, Document},
    cmap::{Command, CommandResponse, StreamDescription},
    coll::{options::DistinctOptions, Namespace},
    error::Result,
    operation::{append_options, Operation, Retryability},
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

impl Operation for Distinct {
    type O = Vec<Bson>;
    const NAME: &'static str = "distinct";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let mut body: Document = doc! {
            Self::NAME: self.ns.coll.clone(),
            "key": self.field_name.clone(),
        };

        if let Some(ref query) = self.query {
            body.insert("query", query.clone());
        }

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }
    fn handle_response(
        &self,
        response: CommandResponse,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.body::<ResponseBody>().map(|body| body.values)
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
}

#[derive(Debug, Deserialize)]
struct ResponseBody {
    values: Vec<Bson>,
}
