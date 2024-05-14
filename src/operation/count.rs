use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    error::{Error, Result},
    operation::{append_options, OperationWithDefaults, Retryability},
    selection_criteria::SelectionCriteria,
};

use super::ExecutionContext;

pub(crate) struct Count {
    ns: Namespace,
    options: Option<EstimatedDocumentCountOptions>,
}

impl Count {
    pub fn new(ns: Namespace, options: Option<EstimatedDocumentCountOptions>) -> Self {
        Count { ns, options }
    }
}

impl OperationWithDefaults for Count {
    type O = u64;
    type Command = Document;

    const NAME: &'static str = "count";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response_body: ResponseBody = response.body()?;
        Ok(response_body.n)
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(0)
        } else {
            Err(error)
        }
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        if let Some(ref options) = self.options {
            return options.selection_criteria.as_ref();
        }
        None
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseBody {
    n: u64,
}
