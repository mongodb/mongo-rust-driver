#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{append_options, Operation},
    options::DropIndexOptions,
    Namespace,
};

use super::{CommandResponse, EmptyBody};

pub(crate) struct DropIndex {
    ns: Namespace,
    name: String,
    options: Option<DropIndexOptions>,
}

impl DropIndex {
    pub(crate) fn new(
        ns: Namespace,
        name: String,
        options: Option<DropIndexOptions>,
    ) -> Self {
        Self { ns, name, options }
    }
}

impl Operation for DropIndex {
    type O = ();
    type Command = Document;
    type Response = CommandResponse<EmptyBody>;
    const NAME: &'static str = "dropIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "index": self.name.clone(),
        };
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, _response: EmptyBody, _description: &StreamDescription) -> Result<Self::O> {
        Ok(())
    }
}
