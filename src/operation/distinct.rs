use serde::Deserialize;

use crate::{
    bson::{doc, rawdoc, Bson, Document, RawBsonRef, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    coll::{options::DistinctOptions, Namespace},
    error::Result,
    operation::{OperationWithDefaults, Retryability},
    selection_criteria::SelectionCriteria,
};

use super::{append_options_to_raw_document, ExecutionContext};

pub(crate) struct Distinct {
    ns: Namespace,
    field_name: String,
    query: Document,
    options: Option<DistinctOptions>,
}

impl Distinct {
    pub fn new(
        ns: Namespace,
        field_name: String,
        query: Document,
        options: Option<DistinctOptions>,
    ) -> Self {
        Distinct {
            ns,
            field_name,
            query,
            options,
        }
    }
}

impl OperationWithDefaults for Distinct {
    type O = Vec<Bson>;

    const NAME: &'static CStr = cstr!("distinct");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
            "key": self.field_name.clone(),
            "query": RawDocumentBuf::from_document(&self.query)?,
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new_read(
            Self::NAME,
            &self.ns.db,
            self.options.as_ref().and_then(|o| o.read_concern.clone()),
            body,
        ))
    }

    fn extract_at_cluster_time(
        &self,
        response: &crate::bson::RawDocument,
    ) -> Result<Option<crate::bson::Timestamp>> {
        Ok(response
            .get("atClusterTime")?
            .and_then(RawBsonRef::as_timestamp))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
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
