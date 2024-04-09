use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, remove_empty_write_concern, OperationWithDefaults},
    options::{DropIndexOptions, WriteConcern},
    BoxFuture,
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
    type Command = Document;
    const NAME: &'static str = "dropIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "index": self.name.clone(),
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

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
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move { Ok(()) }.boxed()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
