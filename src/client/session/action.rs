use crate::{
    action::{action_impl, StartTransaction},
    error::{ErrorKind, Result},
    sdam::TransactionSupportStatus,
};

use super::TransactionState;

action_impl! {
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
}
