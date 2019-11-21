#[cfg(test)]
mod test;

use bson::{bson, doc, Document};
use serde::Deserialize;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    error::Result,
    operation::{append_options, Operation},
    selection_criteria::SelectionCriteria,
};

pub(crate) struct Count {
    ns: Namespace,
    options: Option<EstimatedDocumentCountOptions>,
}

impl Count {
    pub fn new(ns: Namespace, options: Option<EstimatedDocumentCountOptions>) -> Self {
        Count { ns, options }
    }

    #[allow(dead_code)]
    pub(crate) fn empty() -> Self {
        Count {
            ns: Namespace {
                db: String::new(),
                coll: String::new(),
            },
            options: None,
        }
    }
}

impl Operation for Count {
    type O = i64;
    const NAME: &'static str = "count";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body: Document = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        response.body::<ResponseBody>().map(|body| body.n)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        if let Some(ref options) = self.options {
            return options.selection_criteria.as_ref();
        }
        None
    }
}

#[derive(Debug, Deserialize)]
struct ResponseBody {
    n: i64,
}
