#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{append_options, Operation},
    options::{DropIndexOptions, WriteConcern},
    Namespace,
};

use super::{CommandResponse, EmptyBody};

pub(crate) struct DropIndexes {
    ns: Namespace,
    name: String,
    options: Option<DropIndexOptions>,
}

impl DropIndexes {
    pub(crate) fn new(ns: Namespace, name: String, options: Option<DropIndexOptions>) -> Self {
        Self { ns, name, options }
    }

    #[cfg(test)]
    pub(crate) fn empty() -> Self {
        Self {
            ns: Namespace {
                db: String::new(),
                coll: String::new(),
            },
            name: String::new(),
            options: None,
        }
    }
}

impl Operation for DropIndexes {
    type O = ();
    type Command = Document;
    type Response = CommandResponse<EmptyBody>;
    const NAME: &'static str = "dropIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "index": self.name.clone(),
        };

        let mut options = self.options.clone().unwrap_or_default();
        if self.options.is_some() && *self.write_concern().unwrap() == Default::default() {
            options.write_concern = None;
        }
        append_options(&mut body, Some(&options))?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(
        &self,
        _response: EmptyBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        Ok(())
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
