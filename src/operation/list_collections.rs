use bson::Bson;
use serde::{Deserialize, Serialize};

use crate::{
    bson::{doc, Document},
    cmap::{Command, RawCommandResponse, StreamDescription},
    cursor::CursorSpecification,
    error::Result,
    operation::{append_options, CursorBody, OperationWithDefaults, Retryability},
    options::{ReadPreference, SelectionCriteria},
};

#[derive(Debug)]
pub(crate) struct ListCollections {
    db: String,
    name_only: bool,
    options: Option<ListCollectionsOptions>,
}
/// Specifies the options to a
/// [`Database::list_collections`](../struct.Database.html#method.list_collections) operation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListCollectionsOptions {
    /// The number of documents the server should return per cursor batch.
    ///
    /// Note that this does not have any affect on the documents that are returned by a cursor,
    /// only the number of documents kept in memory at a given time (and by extension, the
    /// number of round trips needed to return the entire set of documents returned by the
    /// query).
    #[serde(
        serialize_with = "crate::serde_util::serialize_u32_option_as_batch_size",
        rename(serialize = "cursor")
    )]
    pub(crate) batch_size: Option<u32>,

    /// Tags the query with an arbitrary [`Bson`] value to help trace the operation through the
    /// database profiler, currentOp and logs.
    ///
    /// This option is only available on server versions 4.4+.
    pub(crate) comment: Option<Bson>,

    pub(crate) filter: Option<Document>,
}

impl ListCollections {
    pub(crate) fn new(
        db: String,
        name_only: bool,
        options: Option<ListCollectionsOptions>,
    ) -> Self {
        Self {
            db,
            name_only,
            options,
        }
    }
}

impl OperationWithDefaults for ListCollections {
    type O = CursorSpecification;
    type Command = Document;

    const NAME: &'static str = "listCollections";

    fn build(&mut self, _description: &StreamDescription) -> Result<Command> {
        let mut body = doc! {
            Self::NAME: 1,
        };

        let mut name_only = self.name_only;
        if let Some(filter) = self.options.as_ref().and_then(|o| o.filter.as_ref()) {
            if name_only && filter.keys().any(|k| k != "name") {
                name_only = false;
            }
        }
        body.insert("nameOnly", name_only);

        append_options(&mut body, self.options.as_ref())?;

        Ok(Command::new(Self::NAME.to_string(), self.db.clone(), body))
    }

    fn handle_response(
        &self,
        raw_response: RawCommandResponse,
        description: &StreamDescription,
    ) -> Result<Self::O> {
        let response: CursorBody = raw_response.body()?;
        Ok(CursorSpecification::new(
            response.cursor,
            description.server_address.clone(),
            self.options.as_ref().and_then(|opts| opts.batch_size),
            None,
            None,
        ))
    }

    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        Some(SelectionCriteria::ReadPreference(ReadPreference::Primary)).as_ref()
    }

    fn retryability(&self) -> Retryability {
        Retryability::Read
    }
}
