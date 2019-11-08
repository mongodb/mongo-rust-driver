#[cfg(test)]
mod test;

use bson::bson;
use bson::doc;

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{Operation, WriteConcernOnlyBody},
    Namespace,
};

#[derive(Debug)]
pub(crate) struct Drop {
    ns: Namespace,
    write_concern: Option<WriteConcern>,
}

impl Drop {
    pub(crate) fn new(ns: Namespace, coll_write_concern: Option<WriteConcern>) -> Self {
        Self {
            ns,
            write_concern: coll_write_concern, // TODO: RUST-35 check options first
        }
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
