use bson::bson;
use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{append_options, Operation, WriteConcernOnlyBody},
    options::CreateCollectionOptions,
    Namespace,
};

#[cfg(test)]
mod test;

#[derive(Debug)]
pub(crate) struct Create {
    ns: Namespace,
    write_concern: Option<WriteConcern>,
    options: Option<CreateCollectionOptions>,
}

impl Create {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: "".to_string(),
                coll: "".to_string(),
            },
            None,
            None,
        )
    }

    pub(crate) fn new(
        ns: Namespace,
        db_write_concern: Option<WriteConcern>,
        options: Option<CreateCollectionOptions>,
    ) -> Self {
        Self {
            ns,
            write_concern: db_write_concern, // TODO: RUST-35 first try options wc
            options,
        }
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

        if let Some(ref wc) = self.write_concern {
            body.insert("writeConcern", wc.to_bson());
        }

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
