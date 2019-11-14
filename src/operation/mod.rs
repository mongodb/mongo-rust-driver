mod create;
mod delete;
mod drop;
mod drop_database;
mod insert;
mod update;

use std::{fmt::Debug, ops::Deref};

use bson::{self, Bson, Document};
use serde::{Deserialize, Serialize};

use crate::{
    cmap::{Command, CommandResponse, StreamDescription},
    error::{BulkWriteError, BulkWriteFailure, ErrorKind, Result, WriteConcernError, WriteFailure},
    sdam::SelectionCriteria,
};

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

        Err(ErrorKind::BulkWriteError(failure).into())
    }
}

impl<T> Deref for WriteResponseBody<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.body
    }
}

#[allow(dead_code)]
#[cfg(test)]
mod test {
    use std::ops::Fn;

    use bson::{bson, doc, Bson};

    use crate::{
        cmap::{CommandResponse, StreamDescription},
        concern::{Acknowledgment, ReadConcern, WriteConcern},
        error::ErrorKind,
        operation::Operation,
        read_preference::ReadPreference,
        sdam::SelectionCriteria,
    };

    pub(crate) fn handle_command_error<T: Operation>(op: T) {
        let cmd_error = CommandResponse::from_document(
            doc! { "ok": 0.0, "code": 123, "codeName": "woops", "errmsg": "My error message" },
        );
        let cmd_error_result = op.handle_response(cmd_error);
        assert!(cmd_error_result.is_err());

        match *cmd_error_result.unwrap_err().kind {
            ErrorKind::CommandError(ref error) => {
                assert_eq!(error.code, 123);
                assert_eq!(error.code_name, "woops");
                assert_eq!(error.message, "My error message")
            }
            _ => panic!("expected command error"),
        };
    }

    fn verify_write_concern<T: Operation>(op: T, wc: Option<WriteConcern>) {
        let cmd = op
            .build(&StreamDescription::new_testing())
            .expect("build should succeed");
        assert_eq!(
            cmd.body.get("writeConcern"),
            wc.map(|wc| wc.to_bson()).as_ref()
        );
    }

    /// Verifies that the given operation builds a command with the appropriate write concern
    /// depending on where it was set.
    ///
    /// In the provided factory function, the first argument is the write concern coming from a
    /// client/db/collection and the second argument is one coming from an options struct.
    pub(crate) fn build_write_concern<F, O>(factory: F)
    where
        O: Operation,
        F: Fn(Option<WriteConcern>, Option<WriteConcern>) -> O,
    {
        let majority = WriteConcern {
            w: Some(Acknowledgment::Majority),
            ..Default::default()
        };

        let w_1 = WriteConcern {
            w: Some(Acknowledgment::Nodes(1)),
            ..Default::default()
        };

        verify_write_concern(factory(None, None), None);
        verify_write_concern(
            factory(Some(majority.clone()), None),
            Some(majority.clone()),
        );
        verify_write_concern(
            factory(Some(majority.clone()), Some(w_1.clone())),
            Some(w_1.clone()),
        );
        verify_write_concern(factory(None, Some(majority.clone())), Some(majority));
    }

    fn verify_read_concern<T: Operation>(op: T, rc: Option<ReadConcern>) {
        let cmd = op
            .build(&StreamDescription::new_testing())
            .expect("build should succeed");
        match rc {
            Some(rc) => assert_eq!(
                cmd.body.get("readConcern"),
                Some(Bson::String(rc.as_str().to_string())).as_ref()
            ),
            None => assert!(cmd.body.get("readConcern").is_none()),
        }
    }

    /// Verifies that the given operation builds a command with the appropriate read concern
    /// depending on where it was set.
    ///
    /// In the provided factory function, the first argument is the read concern coming from a
    /// client/db/collection and the second comes from an options struct.
    pub(crate) fn build_read_concern<F, O>(factory: F)
    where
        O: Operation,
        F: Fn(Option<ReadConcern>, Option<ReadConcern>) -> O,
    {
        verify_read_concern(factory(None, None), None);
        verify_read_concern(
            factory(Some(ReadConcern::Majority), None),
            Some(ReadConcern::Majority),
        );
        verify_read_concern(
            factory(Some(ReadConcern::Majority), Some(ReadConcern::Available)),
            Some(ReadConcern::Available),
        );
        verify_read_concern(
            factory(None, Some(ReadConcern::Available)),
            Some(ReadConcern::Available),
        );
    }

    fn verify_selection_criteria<O: Operation>(
        op: O,
        expected_criteria: Option<SelectionCriteria>,
    ) {
        match (op.selection_criteria(), expected_criteria) {
            (None, None) => {}
            (
                Some(SelectionCriteria::ReadPreference(op_rp)),
                Some(SelectionCriteria::ReadPreference(ref expected_rp)),
            ) => {
                assert_eq!(op_rp, expected_rp);
            }
            (Some(SelectionCriteria::Predicate(_)), Some(SelectionCriteria::Predicate(_))) => {}
            (op_criteria, expected_criteria) => panic!(
                "expected critera {:?}, got {:?}",
                expected_criteria,
                op.selection_criteria()
            ),
        }
    }

    /// Verifies that the given operation returns the appropriate selection criteria.
    ///
    /// In the provided factory function, the first argument is the read preference coming from a
    /// client/db/collection, and the second arg is the criteria coming from an options struct.
    pub(crate) fn op_selection_criteria<F, O>(factory: F)
    where
        O: Operation,
        F: Fn(Option<ReadPreference>, Option<SelectionCriteria>) -> O,
    {
        let primary = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        let nearest = SelectionCriteria::ReadPreference(ReadPreference::Nearest {
            tag_sets: None,
            max_staleness: None,
        });

        verify_selection_criteria(factory(None, None), None);
        verify_selection_criteria(
            factory(primary.as_read_pref().cloned(), None),
            Some(primary.clone()),
        );
        verify_selection_criteria(factory(None, Some(primary.clone())), Some(primary.clone()));
        verify_selection_criteria(
            factory(primary.as_read_pref().cloned(), Some(nearest.clone())),
            Some(nearest),
        );
    }
}
