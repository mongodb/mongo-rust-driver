#![allow(missing_docs)]

use std::collections::HashMap;

use crate::{
    bson::{Bson, Document},
    error::{BulkWriteError, Error, ErrorKind, Result},
    operation::bulk_write::BulkWrite as BulkWriteOperation,
    options::{BulkWriteOptions, WriteConcern, WriteModel},
    results::BulkWriteResult,
    Client,
    ClientSession,
};

use super::{action_impl, option_setters};

impl Client {
    pub fn bulk_write(&self, models: impl IntoIterator<Item = WriteModel>) -> BulkWrite {
        BulkWrite::new(self, models.into_iter().collect())
    }
}

#[must_use]
pub struct BulkWrite<'a> {
    client: &'a Client,
    models: Vec<WriteModel>,
    options: Option<BulkWriteOptions>,
    session: Option<&'a mut ClientSession>,
}

impl<'a> BulkWrite<'a> {
    option_setters!(options: BulkWriteOptions;
        ordered: bool,
        bypass_document_validation: bool,
        comment: Bson,
        let_vars: Document,
        verbose_results: bool,
        write_concern: WriteConcern,
    );

    pub fn session(mut self, session: &'a mut ClientSession) -> BulkWrite<'a> {
        self.session = Some(session);
        self
    }

    fn new(client: &'a Client, models: Vec<WriteModel>) -> Self {
        Self {
            client,
            models,
            options: None,
            session: None,
        }
    }

    fn is_ordered(&self) -> bool {
        self.options
            .as_ref()
            .and_then(|options| options.ordered)
            .unwrap_or(true)
    }
}

#[action_impl]
impl<'a> Action for BulkWrite<'a> {
    type Future = BulkWriteFuture;

    async fn execute(mut self) -> Result<BulkWriteResult> {
        #[cfg(feature = "in-use-encryption-unstable")]
        if self.client.should_auto_encrypt().await {
            use mongocrypt::error::{Error as EncryptionError, ErrorKind as EncryptionErrorKind};

            let error = EncryptionError {
                kind: EncryptionErrorKind::Client,
                code: None,
                message: Some(
                    "bulkWrite does not currently support automatic encryption".to_string(),
                ),
            };
            return Err(ErrorKind::Encryption(error).into());
        }

        resolve_write_concern_with_session!(
            self.client,
            self.options,
            self.session.as_deref_mut()
        )?;

        let mut total_attempted = 0;
        let mut execution_status = ExecutionStatus::None;

        while total_attempted < self.models.len()
            && execution_status.should_continue(self.is_ordered())
        {
            let mut operation = BulkWriteOperation::new(
                self.client.clone(),
                &self.models[total_attempted..],
                total_attempted,
                self.options.as_ref(),
            )
            .await;
            let result = self
                .client
                .execute_operation::<BulkWriteOperation>(
                    &mut operation,
                    self.session.as_deref_mut(),
                )
                .await;
            total_attempted += operation.n_attempted;

            match result {
                Ok(result) => {
                    execution_status = execution_status.with_success(result);
                }
                Err(error) => {
                    execution_status = execution_status.with_failure(error);
                }
            }
        }

        match execution_status {
            ExecutionStatus::Success(bulk_write_result) => Ok(bulk_write_result),
            ExecutionStatus::Error(error) => Err(error),
            ExecutionStatus::None => Err(ErrorKind::InvalidArgument {
                message: "bulk_write must be provided at least one write operation".into(),
            }
            .into()),
        }
    }
}

/// Represents the execution status of a bulk write. The status starts at `None`, indicating that no
/// writes have been attempted yet, and transitions to either `Success` or `Error` as batches are
/// executed. The contents of `Error` can be inspected to determine whether a bulk write can
/// continue with further batches or should be terminated.
enum ExecutionStatus {
    Success(BulkWriteResult),
    Error(Error),
    None,
}

impl ExecutionStatus {
    fn with_success(mut self, result: BulkWriteResult) -> Self {
        match self {
            // Merge two successful sets of results together.
            Self::Success(ref mut current_result) => {
                current_result.merge(result);
                self
            }
            // Merge the results of the new batch into the existing bulk write error.
            Self::Error(ref mut current_error) => {
                let bulk_write_error = Self::get_current_bulk_write_error(current_error);
                bulk_write_error.merge_partial_results(result);
                self
            }
            Self::None => Self::Success(result),
        }
    }

    fn with_failure(self, mut error: Error) -> Self {
        match self {
            // If the new error is a BulkWriteError, merge the successful results into the error's
            // partial result. Otherwise, create a new BulkWriteError with the existing results and
            // set its source as the error that just occurred.
            Self::Success(current_result) => match *error.kind {
                ErrorKind::BulkWrite(ref mut bulk_write_error) => {
                    bulk_write_error.merge_partial_results(current_result);
                    Self::Error(error)
                }
                _ => {
                    let bulk_write_error: Error = ErrorKind::BulkWrite(BulkWriteError {
                        write_errors: HashMap::new(),
                        write_concern_errors: Vec::new(),
                        partial_result: Some(current_result),
                    })
                    .into();
                    Self::Error(bulk_write_error.with_source(error))
                }
            },
            // If the new error is a BulkWriteError, merge its contents with the existing error.
            // Otherwise, set the new error as the existing error's source.
            Self::Error(mut current_error) => match *error.kind {
                ErrorKind::BulkWrite(bulk_write_error) => {
                    let current_bulk_write_error =
                        Self::get_current_bulk_write_error(&mut current_error);
                    current_bulk_write_error.merge(bulk_write_error);
                    Self::Error(current_error)
                }
                _ => Self::Error(current_error.with_source(error)),
            },
            Self::None => Self::Error(error),
        }
    }

    /// Gets a BulkWriteError from a given Error. This method should only be called when adding a
    /// new result or error to the existing state, as it requires that the given Error's kind is
    /// ClientBulkWrite.
    fn get_current_bulk_write_error(error: &mut Error) -> &mut BulkWriteError {
        match *error.kind {
            ErrorKind::BulkWrite(ref mut bulk_write_error) => bulk_write_error,
            _ => unreachable!(),
        }
    }

    /// Whether further bulk write batches should be executed based on the current status of
    /// execution.
    fn should_continue(&self, ordered: bool) -> bool {
        match self {
            Self::Error(ref error) => {
                match *error.kind {
                    ErrorKind::BulkWrite(ref bulk_write_error) => {
                        // A top-level error is always fatal.
                        let top_level_error_occurred = error.source.is_some();
                        // A write error occurring during an ordered bulk write is fatal.
                        let terminal_write_error_occurred =
                            ordered && !bulk_write_error.write_errors.is_empty();

                        !top_level_error_occurred && !terminal_write_error_occurred
                    }
                    // A top-level error is always fatal.
                    _ => false,
                }
            }
            _ => true,
        }
    }
}
