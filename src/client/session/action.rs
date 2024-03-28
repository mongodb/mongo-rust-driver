use crate::{
    action::{action_impl, AbortTransaction, CommitTransaction, StartTransaction},
    error::{ErrorKind, Result},
    operation::{self, Operation},
    sdam::TransactionSupportStatus,
};

use super::TransactionState;

#[action_impl]
impl<'a> Action for StartTransaction<'a> {
    type Future = StartTransactionFuture;

    async fn execute(self) -> Result<()> {
        if self
            .session
            .options
            .as_ref()
            .and_then(|o| o.snapshot)
            .unwrap_or(false)
        {
            return Err(ErrorKind::Transaction {
                message: "Transactions are not supported in snapshot sessions".into(),
            }
            .into());
        }
        match self.session.transaction.state {
            TransactionState::Starting | TransactionState::InProgress => {
                return Err(ErrorKind::Transaction {
                    message: "transaction already in progress".into(),
                }
                .into());
            }
            TransactionState::Committed { .. } => {
                self.session.unpin(); // Unpin session if previous transaction is committed.
            }
            _ => {}
        }
        match self.session.client.transaction_support_status().await? {
            TransactionSupportStatus::Supported => {
                let mut options = match self.options {
                    Some(mut options) => {
                        if let Some(defaults) = self.session.default_transaction_options() {
                            merge_options!(
                                defaults,
                                options,
                                [
                                    read_concern,
                                    write_concern,
                                    selection_criteria,
                                    max_commit_time
                                ]
                            );
                        }
                        Some(options)
                    }
                    None => self.session.default_transaction_options().cloned(),
                };
                resolve_options!(
                    self.session.client,
                    options,
                    [read_concern, write_concern, selection_criteria]
                );

                if let Some(ref options) = options {
                    if !options
                        .write_concern
                        .as_ref()
                        .map(|wc| wc.is_acknowledged())
                        .unwrap_or(true)
                    {
                        return Err(ErrorKind::Transaction {
                            message: "transactions do not support unacknowledged write concerns"
                                .into(),
                        }
                        .into());
                    }
                }

                self.session.increment_txn_number();
                self.session.transaction.start(options);
                Ok(())
            }
            _ => Err(ErrorKind::Transaction {
                message: "Transactions are not supported by this deployment".into(),
            }
            .into()),
        }
    }
}

#[action_impl]
impl<'a> Action for CommitTransaction<'a> {
    type Future = CommitTransactionFuture;

    async fn execute(self) -> Result<()> {
        match &mut self.session.transaction.state {
            TransactionState::None => Err(ErrorKind::Transaction {
                message: "no transaction started".into(),
            }
            .into()),
            TransactionState::Aborted => Err(ErrorKind::Transaction {
                message: "Cannot call commitTransaction after calling abortTransaction".into(),
            }
            .into()),
            TransactionState::Starting => {
                self.session.transaction.commit(false);
                Ok(())
            }
            TransactionState::InProgress => {
                let commit_transaction =
                    operation::CommitTransaction::new(self.session.transaction.options.clone());
                self.session.transaction.commit(true);
                self.session
                    .client
                    .clone()
                    .execute_operation(commit_transaction, self.session)
                    .await
            }
            TransactionState::Committed {
                data_committed: true,
            } => {
                let mut commit_transaction =
                    operation::CommitTransaction::new(self.session.transaction.options.clone());
                commit_transaction.update_for_retry();
                self.session
                    .client
                    .clone()
                    .execute_operation(commit_transaction, self.session)
                    .await
            }
            TransactionState::Committed {
                data_committed: false,
            } => Ok(()),
        }
    }
}

#[action_impl]
impl<'a> Action for AbortTransaction<'a> {
    type Future = AbortTransactionFuture;

    async fn execute(self) -> Result<()> {
        match self.session.transaction.state {
            TransactionState::None => Err(ErrorKind::Transaction {
                message: "no transaction started".into(),
            }
            .into()),
            TransactionState::Committed { .. } => Err(ErrorKind::Transaction {
                message: "Cannot call abortTransaction after calling commitTransaction".into(),
            }
            .into()),
            TransactionState::Aborted => Err(ErrorKind::Transaction {
                message: "cannot call abortTransaction twice".into(),
            }
            .into()),
            TransactionState::Starting => {
                self.session.transaction.abort();
                Ok(())
            }
            TransactionState::InProgress => {
                let write_concern = self
                    .session
                    .transaction
                    .options
                    .as_ref()
                    .and_then(|options| options.write_concern.as_ref())
                    .cloned();
                let abort_transaction = operation::AbortTransaction::new(
                    write_concern,
                    self.session.transaction.pinned.take(),
                );
                self.session.transaction.abort();
                // Errors returned from running an abortTransaction command should be ignored.
                let _result = self
                    .session
                    .client
                    .clone()
                    .execute_operation(abort_transaction, &mut *self.session)
                    .await;
                Ok(())
            }
        }
    }
}
