use super::Client;
use crate::{
    bson::Document,
    client::session::ClusterTime,
    error::Result,
    options::{SessionOptions, TransactionOptions},
    runtime,
    ClientSession as AsyncClientSession,
};

/// A MongoDB client session. This struct represents a logical session used for ordering sequential
/// operations. To create a `ClientSession`, call `start_session` on a
/// [`Client`](../struct.Client.html).
///
/// `ClientSession` instances are not thread safe or fork safe. They can only be used by one thread
/// or process at a time.
pub struct ClientSession {
    pub(crate) async_client_session: AsyncClientSession,
}

impl From<AsyncClientSession> for ClientSession {
    fn from(async_client_session: AsyncClientSession) -> Self {
        Self {
            async_client_session,
        }
    }
}

impl ClientSession {
    /// The client used to create this session.
    pub fn client(&self) -> Client {
        self.async_client_session.client().into()
    }

    /// The id of this session.
    pub fn id(&self) -> &Document {
        self.async_client_session.id()
    }

    /// The highest seen cluster time this session has seen so far.
    /// This will be `None` if this session has not been used in an operation yet.
    pub fn cluster_time(&self) -> Option<&ClusterTime> {
        self.async_client_session.cluster_time()
    }

    /// The options used to create this session.
    pub fn options(&self) -> Option<&SessionOptions> {
        self.async_client_session.options()
    }

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub fn advance_cluster_time(&mut self, to: &ClusterTime) {
        self.async_client_session.advance_cluster_time(to)
    }

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
    /// # let mut session = client.start_session(None)?;
    /// session.start_transaction(None)?;
    /// let result = coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)?;
    /// session.commit_transaction()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn start_transaction(
        &mut self,
        options: impl Into<Option<TransactionOptions>>,
    ) -> Result<()> {
        runtime::block_on(self.async_client_session.start_transaction(options))
    }

    /// Commits the transaction that is currently active on this session.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, sync::{Client, ClientSession}};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com")?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session(None)?;
    /// session.start_transaction(None)?;
    /// let result = coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session)?;
    /// session.commit_transaction()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn commit_transaction(&mut self) -> Result<()> {
        runtime::block_on(self.async_client_session.commit_transaction())
    }

    /// Aborts the transaction that is currently active on this session. Any open transaction will
    /// be aborted automatically in the `Drop` implementation of `ClientSession`.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, sync::{Client, ClientSession, Collection}};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com")?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session(None)?;
    /// session.start_transaction(None)?;
    /// match execute_transaction(coll, &mut session) {
    ///     Ok(_) => session.commit_transaction()?,
    ///     Err(_) => session.abort_transaction()?,
    /// }
    /// # Ok(())
    /// # }
    ///
    /// fn execute_transaction(coll: Collection<Document>, session: &mut ClientSession) -> Result<()> {
    ///     coll.insert_one_with_session(doc! { "x": 1 }, None, session)?;
    ///     coll.delete_one_with_session(doc! { "y": 2 }, None, session)?;
    ///     Ok(())   
    /// }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub fn abort_transaction(&mut self) -> Result<()> {
        runtime::block_on(self.async_client_session.abort_transaction())
    }
}
