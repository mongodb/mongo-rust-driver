#[cfg(test)]
mod test;

use crate::{
    bson::doc,
    cmap::{Command, CommandResponse, StreamDescription},
    error::Result,
    operation::{append_options, Operation, WriteResponseBody},
    options::{CreateIndexesOptions, WriteConcern, Index},
    Namespace,
    results::CreateIndexesResult,
};

#[derive(Debug)]
pub(crate) struct CreateIndexes {
    ns: Namespace,
    indexes: Vec<Index>,
    options: Option<CreateIndexesOptions>,
}

impl CreateIndexes {
    #[cfg(test)]
    fn empty() -> Self {
        Self::new(
            Namespace {
                db: String::new(),
                coll: String::new(),
            },
            Vec::new(),
            None,
        )
    }

    pub(crate) fn new(ns: Namespace, indexes: Vec<Index>, options: Option<CreateIndexesOptions>) -> Self {
        Self { ns, indexes, options }
    }
}

impl Operation for CreateIndexes {
    type O = CreateIndexesResult;
    const NAME: &'static str = "createIndexes";

    fn build(&self, _description: &StreamDescription) -> Result<Command> {
        let indexes = bson::to_bson(&self.indexes)?;
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "indexes": indexes,
        };
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response(&self, response: CommandResponse) -> Result<Self::O> {
        let body = response.body::<WriteResponseBody<Self::O>>()?;
        body.validate()?;
        Ok(body.body)
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
