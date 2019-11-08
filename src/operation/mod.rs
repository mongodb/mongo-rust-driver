use std::{fmt::Debug, ops::Deref};

use bson::{self, Bson, Document};
use serde::{Deserialize, Serialize};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::{BulkWriteError, BulkWriteFailure, ErrorKind, Result, WriteConcernError, WriteFailure},
    sdam::SelectionCriteria,
};

mod create;
mod delete;
mod drop;
mod drop_database;
mod insert;
mod update;

pub(crate) use create::Create;
pub(crate) use delete::Delete;
pub(crate) use drop::Drop;
pub(crate) use drop_database::DropDatabase;
pub(crate) use insert::Insert;
pub(crate) use update::Update;

/// A trait modeling the behavior of a server side operation.
pub(super) trait Operation {
    /// The output type of this operation.
    type O: Debug;

    /// The name of the server side command associated with this operation.
    const NAME: &'static str;

    /// Returns the command that should be sent to the server as part of this operation.
    fn build(&self, description: &StreamDescription) -> Result<Command>;

    /// Interprets the server response to the command.
    fn handle_response(&self, response: CommandResponse) -> Result<Self::O>;

    /// Criteria to use for selecting the server that this operation will be executed on.
    fn selection_criteria(&self) -> Option<&SelectionCriteria> {
        None
    }
}

/// Appends a serializable struct to the input document.
/// The serializable struct MUST serialize to a Document, otherwise an error will be thrown.
#[allow(dead_code)]
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
            Some(ref wc_error) => Err(ErrorKind::WriteError {
                inner: WriteFailure::WriteConcernError(wc_error.clone()),
            }
            .into()),
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
}

impl<T> WriteResponseBody<T> {
    fn validate(&self) -> Result<()> {
        if self.write_errors.is_none() && self.write_concern_error.is_none() {
            return Ok(());
        };

        let failure = BulkWriteFailure {
            write_errors: self.write_errors.clone(),
            write_concern_error: self.write_concern_error.clone(),
        };

        Err(ErrorKind::BulkWriteError { inner: failure }.into())
    }
}

impl<T> Deref for WriteResponseBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

#[cfg(test)]
mod test {
    use bson::{bson, doc};

    use crate::{cmap::CommandResponse, error::ErrorKind, operation::Operation};

    pub(crate) fn handle_command_error<T: Operation>(op: T) {
        let cmd_error = CommandResponse::from_document(
            doc! { "ok": 0.0, "code": 123, "codeName": "woops", "errmsg": "My error message" },
        );
        let cmd_error_result = op.handle_response(cmd_error);
        assert!(cmd_error_result.is_err());

        match *cmd_error_result.unwrap_err().kind {
            ErrorKind::CommandError { inner: ref error } => {
                assert_eq!(error.code, 123);
                assert_eq!(error.code_name, "woops");
                assert_eq!(error.message, "My error message")
            }
            _ => panic!("expected command error"),
        };
    }
}
