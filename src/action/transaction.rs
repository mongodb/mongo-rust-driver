use std::time::Duration;

use crate::{
    client::options::TransactionOptions,
    options::{ReadConcern, WriteConcern},
    selection_criteria::SelectionCriteria,
    ClientSession,
};

use super::option_setters;

impl ClientSession {
    /// Starts a new transaction on this session. If no options are set, the session's
    /// `defaultTransactionOptions` will be used. This session must be passed into each operation
    /// within the transaction; otherwise, the operation will be executed outside of the
    /// transaction.
    ///
    /// Errors returned from operations executed within a transaction may include a
    /// [`crate::error::TRANSIENT_TRANSACTION_ERROR`] label. This label indicates that the entire
    /// transaction can be retried with a reasonable expectation that it will succeed.
    ///
    /// Transactions are supported on MongoDB 4.0+. The Rust driver currently only supports
    /// transactions on replica sets.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client, ClientSession};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session().await?;
    /// session.start_transaction(None).await?;
    /// let result = coll.insert_one(doc! { "x": 1 }).session(&mut session).await?;
    /// session.commit_transaction().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// `await` will return [`Result<()>`].
    pub fn start_transaction(&mut self) -> StartTransaction {
        StartTransaction {
            session: self,
            options: None,
        }
    }

    /// Commits the transaction that is currently active on this session.
    ///
    ///
    /// This method may return an error with a [`crate::error::UNKNOWN_TRANSACTION_COMMIT_RESULT`]
    /// label. This label indicates that it is unknown whether the commit has satisfied the write
    /// concern associated with the transaction. If an error with this label is returned, it is
    /// safe to retry the commit until the write concern is satisfied or an error without the label
    /// is returned.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client, ClientSession};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session().await?;
    /// session.start_transaction(None).await?;
    /// let result = coll.insert_one(doc! { "x": 1 }).session(&mut session).await?;
    /// session.commit_transaction().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// `await` will return [`Result<()>`].
    pub fn commit_transaction_2(&mut self) -> CommitTransaction {
        CommitTransaction { session: self }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::ClientSession {
    /// Starts a new transaction on this session with the given `TransactionOptions`. If no options
    /// are provided, the session's `defaultTransactionOptions` will be used. This session must
    /// be passed into each operation within the transaction; otherwise, the operation will be
    /// executed outside of the transaction.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, sync::{Client, ClientSession}};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com")?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session().run()?;
    /// session.start_transaction(None)?;
    /// let result = coll.insert_one(doc! { "x": 1 }).session(&mut session).run()?;
    /// session.commit_transaction()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`run`](StartTransaction::run) will return [`Result<()>`].
    pub fn start_transaction(&mut self) -> StartTransaction {
        self.async_client_session.start_transaction()
    }

    /// Commits the transaction that is currently active on this session.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, sync::{Client, ClientSession}};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com")?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session().run()?;
    /// session.start_transaction(None)?;
    /// let result = coll.insert_one(doc! { "x": 1 }).session(&mut session).run()?;
    /// session.commit_transaction()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    ///
    /// [`run`](CommitTransaction::run) will return [`Result<()>`].
    pub fn commit_transaction_2(&mut self) -> CommitTransaction {
        self.async_client_session.commit_transaction_2()
    }
}

/// Start a new transaction.  Construct with [`ClientSession::start_transaction`].
#[must_use]
pub struct StartTransaction<'a> {
    pub(crate) session: &'a mut ClientSession,
    pub(crate) options: Option<TransactionOptions>,
}

impl<'a> StartTransaction<'a> {
    option_setters! { options: TransactionOptions;
        read_concern: ReadConcern,
        write_concern: WriteConcern,
        selection_criteria: SelectionCriteria,
        max_commit_time: Duration,
    }
}

/// Commits a currently-active transaction.  Construct with [`ClientSession::commit_transaction`].
#[must_use]
pub struct CommitTransaction<'a> {
    pub(crate) session: &'a mut ClientSession,
}

// Action impls at src/client/session/action.rs
