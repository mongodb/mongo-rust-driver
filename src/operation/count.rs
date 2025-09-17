use crate::bson::rawdoc;
use serde::Deserialize;

use crate::{
    bson::doc,
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::EstimatedDocumentCountOptions, Namespace},
    error::{Error, Result},
    operation::{OperationWithDefaults, Retryability},
    selection_criteria::SelectionCriteria,
};

use super::{append_options_to_raw_document, ExecutionContext};

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

    const NAME: &'static CStr = cstr!("count");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            Self::NAME,
            &self.ns.db,
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
