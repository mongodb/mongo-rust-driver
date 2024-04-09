use futures_util::FutureExt;

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{ErrorKind, Result},
    index::IndexModel,
    operation::{append_options, remove_empty_write_concern, OperationWithDefaults},
    options::{CreateIndexOptions, WriteConcern},
    results::CreateIndexesResult,
    BoxFuture,
    ClientSession,
    Namespace,
};

use super::WriteConcernOnlyBody;

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
}

impl OperationWithDefaults for CreateIndexes {
    type O = CreateIndexesResult;
    type Command = Document;
    const NAME: &'static str = "createIndexes";

    fn build(&mut self, description: &StreamDescription) -> Result<Command> {
        // commit quorum is not supported on < 4.4
        if description.max_wire_version.unwrap_or(0) < 9
            && self
                .options
                .as_ref()
                .map_or(false, |options| options.commit_quorum.is_some())
        {
            return Err(ErrorKind::InvalidArgument {
                message: "Specifying a commit quorum to create_index(es) is not supported on \
                          server versions < 4.4"
                    .to_string(),
            }
            .into());
        }

        self.indexes.iter_mut().for_each(|i| i.update_name()); // Generate names for unnamed indexes.
        let indexes = bson::to_bson(&self.indexes)?;
        let mut body = doc! {
            Self::NAME: self.ns.coll.clone(),
            "indexes": indexes,
        };

        remove_empty_write_concern!(self.options);
        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(
            Self::NAME.to_string(),
            self.ns.db.clone(),
            body,
        ))
    }

    fn handle_response<'a>(
        &'a self,
        response: RawCommandResponse,
        _description: &'a StreamDescription,
        _session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Self::O>> {
        async move {
            let response: WriteConcernOnlyBody = response.body()?;
            response.validate()?;
            let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
            Ok(CreateIndexesResult { index_names })
        }
        .boxed()
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
