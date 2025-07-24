use crate::bson::rawdoc;

use crate::{
    bson_compat::{cstr, CStr},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options_to_raw_document, OperationWithDefaults, WriteConcernOnlyBody},
    options::{CreateCollectionOptions, WriteConcern},
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    pub(crate) fn new(ns: Namespace, options: Option<CreateCollectionOptions>) -> Self {
        Self { ns, options }
    }
}

impl OperationWithDefaults for Create {
    type O = ();

    const NAME: &'static CStr = cstr!("create");

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME, &self.ns.db, body))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
