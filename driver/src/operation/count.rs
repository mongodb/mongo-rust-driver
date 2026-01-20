use crate::{bson::rawdoc, options::SelectionCriteria, Collection};
use serde::Deserialize;

use crate::{
    bson::{doc, Document},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::options::EstimatedDocumentCountOptions,
    error::{Error, Result},
    operation::{OperationWithDefaults, Retryability},
};

use super::{append_options_to_raw_document, ExecutionContext};

pub(crate) struct Count {
    target: Collection<Document>,
    options: Option<EstimatedDocumentCountOptions>,
}

impl Count {
    pub fn new(
        target: Collection<Document>,
        options: Option<EstimatedDocumentCountOptions>,
    ) -> Self {
        Count { target, options }
    }
}

impl OperationWithDefaults for Count {
    type O = u64;

    const NAME: &'static CStr = cstr!("count");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            Self::NAME,
            self.target.db().name(),
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
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

    fn selection_criteria(&self) -> super::Feature<&SelectionCriteria> {
        self.options
            .as_ref()
            .and_then(|o| o.selection_criteria.as_ref())
            .into()
    }

    fn supports_read_concern(&self, _description: &StreamDescription) -> bool {
        true
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }

    fn target(&self) -> super::OperationTarget {
        (&self.target).into()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for Count {}

#[derive(Debug, Deserialize)]
pub(crate) struct ResponseBody {
    n: u64,
}
