#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    error::Result,
    operation::{append_options, Operation},
    options::{CreateIndexesOptions, WriteConcern},
    IndexModel,
    Namespace,
};

use super::{CommandResponse, CreateIndexResponseBody};

#[derive(Debug)]
pub(crate) struct CreateIndexes {
    ns: Namespace,
    indexes: Vec<IndexModel>,
    options: Option<CreateIndexesOptions>,
}

impl CreateIndexes {
    pub(crate) fn new(
        ns: Namespace,
        indexes: Vec<IndexModel>,
        options: Option<CreateIndexesOptions>,
    ) -> Self {
        Self {
            ns,
            indexes,
            options,
        }
    }
}

impl Operation for CreateIndexes {
    type O = Vec<String>;
    type Command = Document;
    type Response = CommandResponse<CreateIndexResponseBody>;
    const NAME: &'static str = "createIndexes";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        self.indexes.iter_mut().for_each(|i| i.update_name()); // Generate names for unnamed indexes.
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

    fn handle_response(
        &self,
        _response: CreateIndexResponseBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        Ok(index_names)
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
