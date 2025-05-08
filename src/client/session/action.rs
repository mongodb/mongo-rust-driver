use std::time::{Duration, Instant};

use crate::{
    action::{action_impl, AbortTransaction, CommitTransaction, StartTransaction},
    client::options::TransactionOptions,
    error::{ErrorKind, Result},
    operation::{self, Operation},
    sdam::TransactionSupportStatus,
    BoxFuture,
    ClientSession,
};

use super::TransactionState;

impl ClientSession {
    async fn start_transaction_impl(&mut self, options: Option<TransactionOptions>) -> Result<()> {
        if self
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
        match self.transaction.state {
            TransactionState::Starting | TransactionState::InProgress => {
                return Err(ErrorKind::Transaction {
                    message: "transaction already in progress".into(),
                }
                .into());
            }
            TransactionState::Committed { .. } => {
                self.unpin(); // Unpin session if previous transaction is committed.
            }
            _ => {}
        }
        match self.client.transaction_support_status().await? {
            TransactionSupportStatus::Supported => {
                let mut options = match options {
                    Some(mut options) => {
                        if let Some(defaults) = self.default_transaction_options() {
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
                    None => self.default_transaction_options().cloned(),
                };
                resolve_options!(
                    self.client,
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

                self.increment_txn_number();
                self.transaction.start(options);
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
impl<'a> Action for StartTransaction<&'a mut ClientSession> {
    type Future = StartTransactionFuture;

    async fn execute(self) -> Result<()> {
        self.session.start_transaction_impl(self.options).await
    }
}

macro_rules! convenient_run {
    (
        $session:expr,
        $start_transaction:expr,
        $callback:expr,
        $abort_transaction:expr,
        $commit_transaction:expr,
    ) => {{
        let timeout = Duration::from_secs(120);
        #[cfg(test)]
        let timeout = $session.convenient_transaction_timeout.unwrap_or(timeout);
        let start = Instant::now();

        use crate::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};

        'transaction: loop {
            $start_transaction?;
            let ret = match $callback {
                Ok(v) => v,
                Err(e) => {
                    if matches!(
                        $session.transaction.state,
                        TransactionState::Starting | TransactionState::InProgress
                    ) {
                        $abort_transaction?;
                    }
                    if e.contains_label(TRANSIENT_TRANSACTION_ERROR) && start.elapsed() < timeout {
                        continue 'transaction;
                    }
                    return Err(e);
                }
            };
            if matches!(
                $session.transaction.state,
                TransactionState::None
                    | TransactionState::Aborted
                    | TransactionState::Committed { .. }
            ) {
                return Ok(ret);
            }
            'commit: loop {
                match $commit_transaction {
                    Ok(()) => return Ok(ret),
                    Err(e) => {
                        if e.is_max_time_ms_expired_error() || start.elapsed() >= timeout {
                            return Err(e);
                        }
                        if e.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
                            continue 'commit;
                        }
                        if e.contains_label(TRANSIENT_TRANSACTION_ERROR) {
                            continue 'transaction;
                        }
                        return Err(e);
                    }
                }
            }
        }
    }};
}

impl StartTransaction<&mut ClientSession> {
    /// Starts a transaction, runs the given callback, and commits or aborts the transaction.
    /// Transient transaction errors will cause the callback or the commit to be retried;
    /// other errors will cause the transaction to be aborted and the error returned to the
    /// caller.  If the callback needs to provide its own error information, the
    /// [`Error::custom`](crate::error::Error::custom) method can accept an arbitrary payload that
    /// can be retrieved via [`Error::get_custom`](crate::error::Error::get_custom).
    ///
    /// If a command inside the callback fails, it may cause the transaction on the server to be
    /// aborted. This situation is normally handled transparently by the driver. However, if the
    /// application does not return that error from the callback, the driver will not be able to
    /// determine whether the transaction was aborted or not. The driver will then retry the
    /// callback indefinitely. To avoid this situation, the application MUST NOT silently handle
    /// errors within the callback. If the application needs to handle errors within the
    /// callback, it MUST return them after doing so.
    ///
    /// Because the callback can be repeatedly executed and because it returns a future, the rust
    /// closure borrowing rules for captured values can be overly restrictive.  As a
    /// convenience, `and_run` accepts a context argument that will be passed to the
    /// callback along with the session:
    ///
    /// ```no_run
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client};
    /// # use futures::FutureExt;
    /// # async fn wrapper() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let mut session = client.start_session().await?;
    /// let coll = client.database("mydb").collection::<Document>("mycoll");
    /// let my_data = "my data".to_string();
    /// // This works:
    /// session.start_transaction().and_run(
    ///     (&coll, &my_data),
    ///     |session, (coll, my_data)| async move {
    ///         coll.insert_one(doc! { "data": *my_data }).session(session).await
    ///     }.boxed()
    /// ).await?;
    /// /* This will not compile with a "variable moved due to use in generator" error:
    /// session.start_transaction().and_run(
    ///     (),
    ///     |session, _| async move {
    ///         coll.insert_one(doc! { "data": my_data }).session(session).await
    ///     }.boxed()
    /// ).await?;
    /// */
    /// # Ok(())
    /// # }
    /// ```
    #[rustversion::attr(since(1.85), deprecated = "use and_run2")]
    pub async fn and_run<R, C, F>(self, mut context: C, mut callback: F) -> Result<R>
    where
        F: for<'b> FnMut(&'b mut ClientSession, &'b mut C) -> BoxFuture<'b, Result<R>>,
    {
        convenient_run!(
            self.session,
            self.session
                .start_transaction()
                .with_options(self.options.clone())
                .await,
            callback(self.session, &mut context).await,
            self.session.abort_transaction().await,
            self.session.commit_transaction().await,
        )
    }

    /// Starts a transaction, runs the given callback, and commits or aborts the transaction.
    /// Transient transaction errors will cause the callback or the commit to be retried;
    /// other errors will cause the transaction to be aborted and the error returned to the
    /// caller.  If the callback needs to provide its own error information, the
    /// [`Error::custom`](crate::error::Error::custom) method can accept an arbitrary payload that
    /// can be retrieved via [`Error::get_custom`](crate::error::Error::get_custom).
    ///
    /// If a command inside the callback fails, it may cause the transaction on the server to be
    /// aborted. This situation is normally handled transparently by the driver. However, if the
    /// application does not return that error from the callback, the driver will not be able to
    /// determine whether the transaction was aborted or not. The driver will then retry the
    /// callback indefinitely. To avoid this situation, the application MUST NOT silently handle
    /// errors within the callback. If the application needs to handle errors within the
    /// callback, it MUST return them after doing so.
    ///
    /// This version of the method uses an async closure, which means it's both more convenient and
    /// avoids the lifetime issues of `and_run`, but is only available in Rust versions 1.85 and
    /// above.
    ///
    /// Because the callback can be repeatedly executed, code within the callback cannot consume
    /// owned values, even values owned by the callback itself:
    ///
    /// ```no_run
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client};
    /// # use futures::FutureExt;
    /// # async fn wrapper() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let mut session = client.start_session().await?;
    /// let coll = client.database("mydb").collection::<Document>("mycoll");
    /// let my_data = "my data".to_string();
    /// // This works:
    /// session.start_transaction().and_run2(
    ///     async move |session| {
    ///         coll.insert_one(doc! { "data": my_data.clone() }).session(session).await
    ///     }
    /// ).await?;
    /// /* This will not compile:
    /// session.start_transaction().and_run2(
    ///     async move |session| {
    ///         coll.insert_one(doc! { "data": my_data }).session(session).await
    ///     }
    /// ).await?;
    /// */
    /// # Ok(())
    /// # }
    /// ```
    #[rustversion::since(1.85)]
    pub async fn and_run2<R>(
        self,
        mut callback: impl AsyncFnMut(&mut ClientSession) -> Result<R>,
    ) -> Result<R> {
        convenient_run!(
            self.session,
            self.session
                .start_transaction()
                .with_options(self.options.clone())
                .await,
            callback(self.session).await,
            self.session.abort_transaction().await,
            self.session.commit_transaction().await,
        )
    }
}

#[cfg(feature = "sync")]
impl StartTransaction<&mut crate::sync::ClientSession> {
    /// Synchronously execute this action.
    pub fn run(self) -> Result<()> {
        crate::sync::TOKIO_RUNTIME.block_on(
            self.session
                .async_client_session
                .start_transaction_impl(self.options),
        )
    }

    /// Starts a transaction, runs the given callback, and commits or aborts the transaction.
    /// Transient transaction errors will cause the callback or the commit to be retried;
    /// other errors will cause the transaction to be aborted and the error returned to the
    /// caller.  If the callback needs to provide its own error information, the
    /// [`Error::custom`](crate::error::Error::custom) method can accept an arbitrary payload that
    /// can be retrieved via [`Error::get_custom`](crate::error::Error::get_custom).
    ///
    /// If a command inside the callback fails, it may cause the transaction on the server to be
    /// aborted. This situation is normally handled transparently by the driver. However, if the
    /// application does not return that error from the callback, the driver will not be able to
    /// determine whether the transaction was aborted or not. The driver will then retry the
    /// callback indefinitely. To avoid this situation, the application MUST NOT silently handle
    /// errors within the callback. If the application needs to handle errors within the
    /// callback, it MUST return them after doing so.
    pub fn and_run<R, F>(self, mut callback: F) -> Result<R>
    where
        F: for<'b> FnMut(&'b mut crate::sync::ClientSession) -> Result<R>,
    {
        convenient_run!(
            self.session.async_client_session,
            self.session
                .start_transaction()
                .with_options(self.options.clone())
                .run(),
            callback(self.session),
            self.session.abort_transaction().run(),
            self.session.commit_transaction().run(),
        )
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
