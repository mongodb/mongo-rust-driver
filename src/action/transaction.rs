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
    pub fn start_transaction_2(&mut self) -> StartTransaction {
        StartTransaction {
            session: self,
            options: None,
        }
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
    pub fn start_transaction_2(&mut self) -> StartTransaction {
        self.async_client_session.start_transaction_2()
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

// Action impl at src/client/session/action.rs
