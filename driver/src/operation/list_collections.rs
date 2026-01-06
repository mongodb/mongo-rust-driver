use crate::bson::rawdoc;

use crate::{
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{OperationWithDefaults, Retryability},
    options::{ListCollectionsOptions, ReadPreference, SelectionCriteria},
};

use super::{append_options_to_raw_document, ExecutionContext};

#[derive(Debug)]
pub(crate) struct ListCollections {
    db: String,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
}

impl ListCollections {
    pub(crate) fn new(
        db: String,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            name_only,
            options,
        }
    }
}

impl OperationWithDefaults for ListCollections {
    type O = CursorSpecification;

    const NAME: &'static CStr = cstr!("listCollections");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(filter) = self.options.as_ref().and_then(|o| o.filter.as_ref()) {
            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.append(cstr!("nameOnly"), name_only);

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, &self.db, body))
    }

    fn handle_response_cow<'a>(
        &'a self,
        response: std::borrow::Cow<'a, RawCommandResponse>,
        context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        CursorSpecification::new(
            response.into_owned(),
            context
                .connection
                .stream_description()?
                .server_address
                .clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
            None,
        )
    }

    fn wants_owned_response(&self) -> bool {
        true
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
impl crate::otel::OtelInfoDefaults for ListCollections {
    fn output_cursor_id(output: &Self::O) -> Option<i64> {
        Some(output.id())
    }

    #[cfg(feature = "opentelemetry")]
    fn target(&self) -> crate::otel::OperationTarget<'_> {
        self.db.as_str().into()
    }
}
