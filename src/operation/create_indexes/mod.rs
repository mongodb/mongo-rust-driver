#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    error::Result,
    index::IndexModel,
    operation::{append_options, Operation},
    options::{CreateIndexOptions, WriteConcern},
    results::CreateIndexesResult,
    Namespace,
};

use super::{CommandResponse, WriteConcernOnlyBody};

#[derive(Debug)]
pub(crate) struct CreateIndexes {
    ns: Namespace,
    indexes: Vec<IndexModel>,
    options: Option<CreateIndexOptions>,
}

impl CreateIndexes {
    pub(crate) fn new(
        ns: Namespace,
        indexes: Vec<IndexModel>,
        options: Option<CreateIndexOptions>,
    ) -> Self {
        Self {
            ns,
            indexes,
            options,
        }
    }

    #[cfg(test)]
    pub(crate) fn with_indexes(indexes: Vec<IndexModel>) -> Self {
        Self {
            ns: Namespace {
                db: String::new(),
                coll: String::new(),
            },
            indexes,
            options: None,
        }
    }
}

impl Operation for CreateIndexes {
    type O = CreateIndexesResult;
    type Command = Document;
    type Response = CommandResponse<WriteConcernOnlyBody>;
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
        response: WriteConcernOnlyBody,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        response.validate()?;
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        Ok(CreateIndexesResult { index_names })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
