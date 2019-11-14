#[cfg(test)]
mod test;

use bson::{bson, doc};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation, WriteConcernOnlyBody},
    options::DropCollectionOptions,
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Drop {
    ns: Namespace,
    options: Option<DropCollectionOptions>,
}

impl Drop {
    pub(crate) fn new(ns: Namespace, options: Option<DropCollectionOptions>) -> Self {
        Self { ns, options }
    }

    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            None,
        )
    }
}

impl Operation for Drop {
    type O = ();
    const NAME: &'static str = "drop";

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
}
