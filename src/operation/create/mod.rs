#[cfg(test)]
mod test;

use bson::Document;

use crate::{
    bson::doc,
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{append_options, Operation, WriteConcernOnlyBody},
    options::{CreateCollectionOptions, WriteConcern},
    Namespace,
};

use super::CommandResponse;

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            None,
        )
    }

    pub(crate) fn new(ns: Namespace, options: Option<CreateCollectionOptions>) -> Self {
        Self { ns, options }
    }
}

impl Operation for Create {
    type O = ();
    type Command = Document;
    type Response = CommandResponse<WriteConcernOnlyBody>;

    const NAME: &'static str = "create";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };

        let mut options = self.options.clone().unwrap_or_default();
        if let Some(write_concern) = self.write_concern() {
            if *write_concern == Default::default() {
                options.write_concern = None;
            }
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
        response: WriteConcernOnlyBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
