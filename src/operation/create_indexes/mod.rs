#[cfg(test)]
mod test;

use crate::{
    bson::{doc, Document},
    cmap::{Command, StreamDescription},
    coll::options::CommitQuorum,
    error::Result,
    index::IndexModel,
    operation::{append_options, Operation},
    options::{CreateIndexOptions, WriteConcern},
    results::CreateIndexesResult,
    Namespace,
};

use super::CommandResponse;
use serde::{
    de::{Error, Unexpected},
    Deserialize,
    Deserializer,
};

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
    type Response = CommandResponse<Response>;
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
        response: Response,
        _description: &StreamDescription,
    ) -> Result<Self::O> {
        let index_names = self.indexes.iter().filter_map(|i| i.get_name()).collect();
        let Response(response) = response;
        Ok(CreateIndexesResult {
            index_names,
            created_collection_automatically: response.created_collection_automatically,
            num_indexes_before: response.num_indexes_before,
            num_indexes_after: response.num_indexes_after,
            note: response.note,
            commit_quorum: response.commit_quorum,
        })
    }

    fn write_concern(&self) -> Option<&WriteConcern> {
        self.options
            .as_ref()
            .and_then(|opts| opts.write_concern.as_ref())
    }
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResponseBody {
    created_collection_automatically: Option<bool>,
    num_indexes_before: u32,
    num_indexes_after: u32,
    note: Option<String>,
    commit_quorum: Option<CommitQuorum>,
}

#[derive(Debug)]
pub(crate) struct Response(ResponseBody);

impl<'de> Deserialize<'de> for Response {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum ResponseHelper {
            PlainResponse(ResponseBody),
            ShardedResponse { raw: Document },
        }

        match ResponseHelper::deserialize(deserializer)? {
            ResponseHelper::PlainResponse(body) => Ok(Response(body)),
            ResponseHelper::ShardedResponse { raw } => {
                let len = raw.values().count();
                if len != 1 {
                    return Err(Error::invalid_length(len, &"a single result"));
                }

                let (_, v) = raw.into_iter().next().unwrap(); // Safe unwrap because of length check above.
                bson::from_bson(v).map(Response).map_err(|_| {
                    Error::invalid_type(
                        Unexpected::Other("Unknown bson"),
                        &"a createIndexes response",
                    )
                })
            }
        }
    }
}
