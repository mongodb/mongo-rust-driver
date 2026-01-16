use crate::bson::rawdoc;
use serde::Deserialize;

use crate::{
    bson::{doc, RawDocumentBuf},
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    db::options::ListDatabasesOptions,
    error::Result,
    operation::{OperationWithDefaults, Retryability},
    selection_criteria::{ReadPreference, SelectionCriteria},
};

use super::{append_options_to_raw_document, ExecutionContext};

#[derive(Debug)]
pub(crate) struct ListDatabases {
    name_only: bool,
    options: Option<ListDatabasesOptions>,
}

impl ListDatabases {
    pub fn new(name_only: bool, options: Option<ListDatabasesOptions>) -> Self {
        ListDatabases { name_only, options }
    }
}

impl OperationWithDefaults for ListDatabases {
    type O = Vec<RawDocumentBuf>;

    const NAME: &'static CStr = cstr!("listDatabases");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
            "nameOnly": self.name_only
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, "admin", body))
    }

    fn handle_response<'a>(
        &'a self,
        response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: Response = response.body()?;
        Ok(response.databases)
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for ListDatabases {
    fn target(&self) -> crate::otel::TargetName<'_> {
        crate::otel::TargetName::ADMIN
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    databases: Vec<RawDocumentBuf>,
}
