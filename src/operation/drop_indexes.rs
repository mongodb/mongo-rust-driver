use bson::rawdoc;

use crate::{
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{
        append_options_to_raw_document,
        remove_empty_write_concern,
        OperationWithDefaults,
    },
    options::{DropIndexOptions, WriteConcern},
    Namespace,
};

use super::ExecutionContext;

pub(crate) struct DropIndexes {
    ns: Namespace,
    name: String,
    options: Option<DropIndexOptions>,
}

impl DropIndexes {
    pub(crate) fn new(ns: Namespace, name: String, options: Option<DropIndexOptions>) -> Self {
        Self { ns, name, options }
    }
}

impl OperationWithDefaults for DropIndexes {
    type O = ();
    const NAME: &'static str = "dropIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = rawdoc! {
            Self::NAME: self.ns.coll.clone(),
            "index": self.name.clone(),
        };

        remove_empty_write_concern!(self.options);
        append_options_to_raw_document(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        _response: RawCommandResponse,
        _context: ExecutionContext<'a>,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
