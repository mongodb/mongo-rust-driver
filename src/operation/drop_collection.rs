use crate::bson::rawdoc;

use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{Error, Result},
    operation::{append_options_to_raw_document, OperationWithDefaults, WriteConcernOnlyBody},
    options::{DropCollectionOptions, WriteConcern},
    Namespace,
};

use super::ExecutionContext;

#[derive(Debug)]
pub(crate) struct DropCollection {
    ns: Namespace,
    options: Option<DropCollectionOptions>,
}

impl DropCollection {
    pub(crate) fn new(ns: Namespace, options: Option<DropCollectionOptions>) -> Self {
        DropCollection { ns, options }
    }
}

impl OperationWithDefaults for DropCollection {
    type O = ();

    const NAME: &'static str = "drop";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
        };

        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        let response: WriteConcernOnlyBody = response.body()?;
        response.validate()
    }

    fn handle_error(&self, error: Error) -> Result<Self::O> {
        if error.is_ns_not_found() {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
