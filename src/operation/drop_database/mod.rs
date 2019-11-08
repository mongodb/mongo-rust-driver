#[cfg(test)]
mod test;

use bson::{bson, doc};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    concern::WriteConcern,
    error::Result,
    operation::{Operation, WriteConcernOnlyBody},
};

#[derive(Debug)]
pub(crate) struct DropDatabase {
    target_db: String,
    write_concern: Option<WriteConcern>,
}

impl DropDatabase {
    #[allow(dead_code)]
    fn empty() -> Self {
        Self::new(String::new(), None)
    }

    pub(crate) fn new(target_db: String, write_concern: Option<WriteConcern>) -> Self {
        // TODO: RUST-35 use write concern from options ?
        Self {
            target_db,
            write_concern,
        }
    }
}

impl Operation for DropDatabase {
    type O = ();
    const NAME: &'static str = "dropDatabase";

    fn build(&self, description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };
        if let Some(ref wc) = self.write_concern {
            body.insert("writeConcern", wc.to_bson());
        }

        Ok(Command::new(
            Self::NAME.to_string(),
            self.target_db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        response.body::<WriteConcernOnlyBody>()?.validate()
    }
}
