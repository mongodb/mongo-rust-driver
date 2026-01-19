use crate::{bson::rawdoc, Collection};

use crate::{
    bson::Document,
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options_to_raw_document, OperationWithDefaults},
    options::{DropIndexOptions, WriteConcern},
};

use super::ExecutionContext;

pub(crate) struct DropIndexes {
    target: Collection<Document>,
    name: String,
    options: Option<DropIndexOptions>,
}

impl DropIndexes {
    pub(crate) fn new(
        target: Collection<Document>,
        name: String,
        options: Option<DropIndexOptions>,
    ) -> Self {
        Self {
            target,
            name,
            options,
        }
    }
}

impl OperationWithDefaults for DropIndexes {
    type O = ();
    const NAME: &'static CStr = cstr!("dropIndexes");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.target.name(),
            "index": self.name.clone(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, &self.target.db().name(), body))
    }

    fn handle_response<'a>(
        &'a self,
        _response: &'a RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }

    fn target(&self) -> super::OperationTarget {
        (&self.target).into()
    }

    #[cfg(feature = "opentelemetry")]
    type Otel = crate::otel::Witness<Self>;
}

#[cfg(feature = "opentelemetry")]
impl crate::otel::OtelInfoDefaults for DropIndexes {}
