mod aggregate;
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

use std::{collections::VecDeque, fmt::Debug, ops::Deref};

use serde::{Deserialize, Serialize};

use crate::{
    bson::{self, Bson, Document},
    cmap::{Command, CommandResponse, StreamDescription},
    error::{
        BulkWriteError,
        BulkWriteFailure,
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

pub(crate) use aggregate::Aggregate;
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
    type O: Debug;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    fn build(&self, description: &StreamDescription) -> Result<Command>;

    /// Interprets the server response to the command.
    fn handle_response(
        &self,
        response: CommandResponse,
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
}

/// Appends a serializable struct to the input document.
/// The serializable struct MUST serialize to a Document, otherwise an error will be thrown.
pub(crate) fn append_options<T: Serialize>(doc: &mut Document, options: Option<&T>) -> Result<()> {
    match options {
        Some(options) => {
            let temp_doc = bson::to_bson(options)?;
            match temp_doc {
                Bson::Document(d) => {
                    doc.extend(d);
                    Ok(())
                }
                _ => Err(ErrorKind::OperationError {
                    message: "options did not serialize to a Document".to_string(),
                }
                .into()),
            }
        }
        None => Ok(()),
    }
}

#[derive(Deserialize)]
struct EmptyBody {}

/// Body of a write response that could possibly have a write concern error but not write errors.
#[derive(Deserialize)]
struct WriteConcernOnlyBody {
    #[serde(rename = "writeConcernError")]
    write_concern_error: Option<WriteConcernError>,
}

impl WriteConcernOnlyBody {
    fn validate(&self) -> Result<()> {
        match self.write_concern_error {
            Some(ref wc_error) => {
                Err(ErrorKind::WriteError(WriteFailure::WriteConcernError(wc_error.clone())).into())
            }
            None => Ok(()),
        }
    }
}

#[derive(Deserialize)]
struct WriteResponseBody<T = EmptyBody> {
    #[serde(flatten)]
    body: T,

    n: i64,

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

        // Error labels for WriteConcernErrors are sent from the server in a separate field.
        let write_concern_error = match self.write_concern_error {
            Some(ref write_concern_error) => {
                let mut write_concern_error = write_concern_error.clone();
                if let Some(ref labels) = self.labels {
                    write_concern_error.labels.append(&mut labels.clone());
                }
                Some(write_concern_error)
            }
            None => None,
        };

        let failure = BulkWriteFailure {
            write_errors: self.write_errors.clone(),
            write_concern_error,
        };

        Err(ErrorKind::BulkWriteError(failure).into())
    }
}

impl<T> Deref for WriteResponseBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

#[derive(Debug, Deserialize)]
struct CursorBody {
    cursor: CursorInfo,
}

#[derive(Debug, Deserialize)]
struct CursorInfo {
    id: i64,
    ns: Namespace,
    #[serde(rename = "firstBatch")]
    first_batch: VecDeque<Document>,
}

#[derive(Debug, PartialEq)]
pub(crate) enum Retryability {
    Write,
    Read,
    None,
}

#[cfg(test)]
mod test {
    use crate::{
        operation::Operation,
        options::{ReadPreference, SelectionCriteria},
    };

    pub(crate) fn op_selection_criteria<F, T>(constructor: F)
    where
        T: Operation,
        F: Fn(Option<SelectionCriteria>) -> T,
    {
        let op = constructor(None);
        assert_eq!(op.selection_criteria(), None);

        let read_pref: SelectionCriteria = ReadPreference::Secondary {
            options: Default::default(),
        }
        .into();

        let op = constructor(Some(read_pref.clone()));
        assert_eq!(op.selection_criteria(), Some(&read_pref));
    }
}
