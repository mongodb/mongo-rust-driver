mod abort_transaction;
mod aggregate;
mod commit_transaction;
mod count;
mod count_documents;
mod create;
mod delete;
mod distinct;
mod drop_collection;
mod drop_database;
mod find;
mod find_and_modify;
mod get_more;
mod insert;
mod list_collections;
mod list_databases;
mod run_command;
mod update;

#[cfg(test)]
mod test;

use std::{collections::VecDeque, fmt::Debug, ops::Deref};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    bson::{self, Bson, Document},
    bson_util,
    client::ClusterTime,
    cmap::{Command, RawCommandResponse, StreamDescription},
    error::{
        BulkWriteError,
        BulkWriteFailure,
        CommandError,
        Error,
        ErrorKind,
        Result,
        WriteConcernError,
        WriteFailure,
    },
    options::WriteConcern,
    selection_criteria::SelectionCriteria,
    Namespace,
};

pub(crate) use abort_transaction::AbortTransaction;
pub(crate) use aggregate::Aggregate;
pub(crate) use commit_transaction::CommitTransaction;
pub(crate) use count::Count;
pub(crate) use count_documents::CountDocuments;
pub(crate) use create::Create;
pub(crate) use delete::Delete;
pub(crate) use distinct::Distinct;
pub(crate) use drop_collection::DropCollection;
pub(crate) use drop_database::DropDatabase;
pub(crate) use find::Find;
pub(crate) use find_and_modify::FindAndModify;
pub(crate) use get_more::GetMore;
pub(crate) use insert::Insert;
pub(crate) use list_collections::ListCollections;
pub(crate) use list_databases::ListDatabases;
pub(crate) use run_command::RunCommand;
pub(crate) use update::Update;

/// A trait modeling the behavior of a server side operation.
pub(crate) trait Operation {
    /// The output type of this operation.
    type O;

    /// The format of the command response from the server.
    type Response: Response;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    /// The operation may store some additional state that is required for handling the response.
    fn build(&mut self, description: &StreamDescription) -> Result<Command>;

    /// Interprets the server response to the command.
    fn handle_response(
        &self,
        response: <Self::Response as Response>::Body,
        description: &StreamDescription,
    ) -> Result<Self::O>;

    /// Interpret an error encountered while sending the built command to the server, potentially
    /// recovering.
    fn handle_error(&self, error: Error) -> Result<Self::O> {
        Err(error)
    }

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        None
    }

    /// Whether or not this operation will request acknowledgment from the server.
    fn is_acknowledged(&self) -> bool {
        self.write_concern()
            .map(WriteConcern::is_acknowledged)
            .unwrap_or(true)
    }

    /// The write concern to use for this operation, if any.
    fn write_concern(&self) -> Option<&WriteConcern> {
        None
    }

    /// Whether this operation supports sessions or not.
    fn supports_sessions(&self) -> bool {
        true
    }

    /// The level of retryability the operation supports.
    fn retryability(&self) -> Retryability {
        Retryability::None
    }

    /// Updates this operation as needed for a retry.
    fn update_for_retry(&mut self) {}

    fn name(&self) -> &str {
        Self::NAME
    }
}

/// Trait modeling the behavior of a command response to a server operation.
pub(crate) trait Response: Sized {
    /// The command-specific portion of a command response.
    /// This type will be passed to the associated operation's `handle_response` method.
    type Body;

    /// Deserialize a response from the given raw response.
    fn deserialize_response(raw: &RawCommandResponse) -> Result<Self>;

    /// The `ok` field of the response.
    fn ok(&self) -> Option<&Bson>;

    /// Whether the command succeeeded or not (i.e. if this response is ok: 1).
    fn is_success(&self) -> bool {
        match self.ok() {
            Some(b) => bson_util::get_int(&b) == Some(1),
            None => false,
        }
    }

    /// The `clusterTime` field of the response.
    fn cluster_time(&self) -> Option<&ClusterTime>;

    /// Convert into the body of the response.
    fn into_body(self) -> Self::Body;
}

/// A response to a command with a body shaped deserialized to a `T`.
#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandResponse<T> {
    pub(crate) ok: Bson,

    #[serde(rename = "$clusterTime")]
    pub(crate) cluster_time: Option<ClusterTime>,

    #[serde(flatten)]
    pub(crate) body: T,
}

impl<T: DeserializeOwned> CommandResponse<T> {
    pub(crate) fn is_success(&self) -> bool {
        <Self as Response>::is_success(self)
    }
}

impl<T: DeserializeOwned> Response for CommandResponse<T> {
    type Body = T;

    fn deserialize_response(raw: &RawCommandResponse) -> Result<Self> {
        raw.body()
    }

    fn ok(&self) -> Option<&Bson> {
        Some(&self.ok)
    }

    fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    fn into_body(self) -> Self::Body {
        self.body
    }
}

/// A response body useful for deserializing command errors.
#[derive(Deserialize, Debug)]
pub(crate) struct CommandErrorBody {
    #[serde(rename = "errorLabels")]
    pub(crate) error_labels: Option<Vec<String>>,

    #[serde(flatten)]
    pub(crate) command_error: CommandError,
}

impl From<CommandErrorBody> for Error {
    fn from(command_error_response: CommandErrorBody) -> Error {
        Error::new(
            ErrorKind::Command(command_error_response.command_error),
            command_error_response.error_labels,
        )
    }
}

/// Appends a serializable struct to the input document.
/// The serializable struct MUST serialize to a Document, otherwise an error will be thrown.
pub(crate) fn append_options<T: Serialize + Debug>(
    doc: &mut Document,
    options: Option<&T>,
) -> Result<()> {
    match options {
        Some(options) => {
            let temp_doc = bson::to_bson(options)?;
            match temp_doc {
                Bson::Document(d) => {
                    doc.extend(d);
                    Ok(())
                }
                _ => Err(ErrorKind::Internal {
                    message: format!("options did not serialize to a Document: {:?}", options),
                }
                .into()),
            }
        }
        None => Ok(()),
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct EmptyBody {}

/// Body of a write response that could possibly have a write concern error but not write errors.
#[derive(Debug, Deserialize, Default, Clone)]
pub(crate) struct WriteConcernOnlyBody {
    #[serde(rename = "writeConcernError")]
    write_concern_error: Option<WriteConcernError>,

    #[serde(rename = "errorLabels")]
    labels: Option<Vec<String>>,
}

impl WriteConcernOnlyBody {
    fn validate(&self) -> Result<()> {
        match self.write_concern_error {
            Some(ref wc_error) => Err(Error::new(
                ErrorKind::Write(WriteFailure::WriteConcernError(wc_error.clone())),
                self.labels.clone(),
            )),
            None => Ok(()),
        }
    }
}

#[derive(Deserialize, Debug)]
pub(crate) struct WriteResponseBody<T = EmptyBody> {
    #[serde(flatten)]
    body: T,

    n: u64,

    #[serde(rename = "writeErrors")]
    write_errors: Option<Vec<BulkWriteError>>,

    #[serde(rename = "writeConcernError")]
    write_concern_error: Option<WriteConcernError>,

    #[serde(rename = "errorLabels")]
    labels: Option<Vec<String>>,
}

impl<T> WriteResponseBody<T> {
    fn validate(&self) -> Result<()> {
        if self.write_errors.is_none() && self.write_concern_error.is_none() {
            return Ok(());
        };

        let failure = BulkWriteFailure {
            write_errors: self.write_errors.clone(),
            write_concern_error: self.write_concern_error.clone(),
            inserted_ids: Default::default(),
        };

        Err(Error::new(
            ErrorKind::BulkWrite(failure),
            self.labels.clone(),
        ))
    }
}

impl<T> Deref for WriteResponseBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct CursorBody<T> {
    cursor: CursorInfo<T>,
}

#[derive(Debug, Deserialize, Clone)]
pub(crate) struct CursorInfo<T> {
    id: i64,
    ns: Namespace,
    #[serde(rename = "firstBatch")]
    first_batch: VecDeque<T>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Retryability {
    Write,
    Read,
    None,
}
