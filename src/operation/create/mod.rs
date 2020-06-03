#[cfg(test)]
mod test;

use crate::{
    bson::doc,
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation, WriteConcernOnlyBody},
    options::{CreateCollectionOptions, WriteConcern},
    Namespace,
};

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
    const NAME: &'static str = "create";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
        };
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        response.body::<WriteConcernOnlyBody>()?.validate()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
