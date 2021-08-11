mod cluster_time;
mod pool;
#[cfg(test)]
mod test;

use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use lazy_static::lazy_static;
use uuid::Uuid;

use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document, Timestamp},
    error::{ErrorKind, Result},
    operation::{AbortTransaction, CommitTransaction, Operation},
    options::{SessionOptions, TransactionOptions},
    sdam::{ServerInfo, TransactionSupportStatus},
    selection_criteria::SelectionCriteria,
    Client,
    RUNTIME,
};
pub(crate) use cluster_time::ClusterTime;
pub(super) use pool::ServerSessionPool;

use super::options::ServerAddress;

lazy_static! {
    pub(crate) static ref SESSIONS_UNSUPPORTED_COMMANDS: HashSet<&'static str> = {
        let mut hash_set = HashSet::new();
        hash_set.insert("killcursors");
        hash_set.insert("parallelcollectionscan");
        hash_set
    };
}

/// A MongoDB client session. This struct represents a logical session used for ordering sequential
/// operations. To create a `ClientSession`, call `start_session` on a `Client`.
///
/// `ClientSession` instances are not thread safe or fork safe. They can only be used by one thread
/// or process at a time.
///
/// ## Transactions
/// Transactions are used to execute a series of operations across multiple documents and
/// collections atomically. For more information about when and how to use transactions in MongoDB,
/// see the [manual](https://docs.mongodb.com/manual/core/transactions/).
///
/// Replica set transactions are supported on MongoDB 4.0+. Sharded transactions are supported on
/// MongoDDB 4.2+. Transactions are associated with a `ClientSession`. To begin a transaction, call
/// [`ClientSession::start_transaction`] on a `ClientSession`. The `ClientSession` must be passed to
/// operations to be executed within the transaction.
///
/// ```rust
/// use mongodb::{
///     bson::{doc, Document},
///     error::{Result, TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
///     options::{Acknowledgment, ReadConcern, TransactionOptions, WriteConcern},
/// #   Client,
///     ClientSession,
///     Collection,
/// };
///
/// # async fn do_stuff() -> Result<()> {
/// # let client = Client::with_uri_str("mongodb://example.com").await?;
/// # let coll: Collection<Document> = client.database("foo").collection("bar");
/// let mut session = client.start_session(None).await?;
/// let options = TransactionOptions::builder()
///     .read_concern(ReadConcern::majority())
///     .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
///     .build();
/// session.start_transaction(options).await?;
/// // A "TransientTransactionError" label indicates that the entire transaction can be retried
/// // with a reasonable expectation that it will succeed.
/// while let Err(error) = execute_transaction(&coll, &mut session).await {
///     if !error.contains_label(TRANSIENT_TRANSACTION_ERROR) {
///         break;
///     }
/// }
/// # Ok(())
/// # }
///
/// async fn execute_transaction(coll: &Collection<Document>, session: &mut ClientSession) -> Result<()> {
///     coll.insert_one_with_session(doc! { "x": 1 }, None, session).await?;
///     coll.delete_one_with_session(doc! { "y": 2 }, None, session).await?;
///     // An "UnknownTransactionCommitResult" label indicates that it is unknown whether the
///     // commit has satisfied the write concern associated with the transaction. If an error
///     // with this label is returned, it is safe to retry the commit until the write concern is
///     // satisfied or an error without the label is returned.
///     loop {
///         let result = session.commit_transaction().await;
///         if let Err(ref error) = result {
///             if error.contains_label(UNKNOWN_TRANSACTION_COMMIT_RESULT) {
///                 continue;
///             }
///         }
///         result?
///     }
/// }
/// ```
#[derive(Clone, Debug)]
pub struct ClientSession {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
    client: Client,
    is_implicit: bool,
    options: Option<SessionOptions>,
    pub(crate) transaction: Transaction,
    pub(crate) snapshot_time: Option<Timestamp>,
}

#[derive(Clone, Debug)]
pub(crate) struct Transaction {
    pub(crate) state: TransactionState,
    pub(crate) options: Option<TransactionOptions>,
    pub(crate) pinned_mongos: Option<SelectionCriteria>,
    pub(crate) recovery_token: Option<Document>,
}

impl Transaction {
    pub(crate) fn start(&mut self, options: Option<TransactionOptions>) {
        self.state = TransactionState::Starting;
        self.options = options;
        self.recovery_token = None;
    }

    pub(crate) fn commit(&mut self, data_committed: bool) {
        self.state = TransactionState::Committed { data_committed };
    }

    pub(crate) fn abort(&mut self) {
        self.state = TransactionState::Aborted;
        self.options = None;
        self.pinned_mongos = None;
    }

    pub(crate) fn reset(&mut self) {
        self.state = TransactionState::None;
        self.options = None;
        self.pinned_mongos = None;
        self.recovery_token = None;
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            state: TransactionState::None,
            options: None,
            pinned_mongos: None,
            recovery_token: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TransactionState {
    None,
    Starting,
    InProgress,
    Committed {
        /// Whether any data was committed when commit_transaction was initially called. This is
        /// required to determine whether a commitTransaction command should be run if the user
        /// calls commit_transaction again.
        data_committed: bool,
    },
    Aborted,
}

impl ClientSession {
    /// Creates a new `ClientSession` wrapping the provided server session.
    pub(crate) fn new(
        server_session: ServerSession,
        client: Client,
        options: Option<SessionOptions>,
        is_implicit: bool,
    ) -> Self {
        Self {
            client,
            server_session,
            cluster_time: None,
            is_implicit,
            options,
            transaction: Default::default(),
            snapshot_time: None,
        }
    }

    /// The client used to create this session.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// The id of this session.
    pub fn id(&self) -> &Document {
        &self.server_session.id
    }

    /// Whether this session was created implicitly by the driver or explcitly by the user.
    pub(crate) fn is_implicit(&self) -> bool {
        self.is_implicit
    }

    /// Whether this session is currently in a transaction.
    pub(crate) fn in_transaction(&self) -> bool {
        self.transaction.state == TransactionState::Starting
            || self.transaction.state == TransactionState::InProgress
    }

    /// The highest seen cluster time this session has seen so far.
    /// This will be `None` if this session has not been used in an operation yet.
    pub fn cluster_time(&self) -> Option<&ClusterTime> {
        self.cluster_time.as_ref()
    }

    /// The options used to create this session.
    pub fn options(&self) -> Option<&SessionOptions> {
        self.options.as_ref()
    }

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub fn advance_cluster_time(&mut self, to: &ClusterTime) {
        if self.cluster_time().map(|ct| ct < to).unwrap_or(true) {
            self.cluster_time = Some(to.clone());
        }
    }

    /// Mark this session (and the underlying server session) as dirty.
    pub(crate) fn mark_dirty(&mut self) {
        self.server_session.dirty = true;
    }

    /// Updates the date that the underlying server session was last used as part of an operation
    /// sent to the server.
    pub(crate) fn update_last_use(&mut self) {
        self.server_session.last_use = Instant::now();
    }

    /// Gets the current txn_number.
    pub(crate) fn txn_number(&self) -> i64 {
        self.server_session.txn_number
    }

    /// Increments the txn_number.
    pub(crate) fn increment_txn_number(&mut self) {
        self.server_session.txn_number += 1;
    }

    /// Increments the txn_number and returns the new value.
    pub(crate) fn get_and_increment_txn_number(&mut self) -> i64 {
        self.increment_txn_number();
        self.server_session.txn_number
    }

    /// Pin mongos to session.
    pub(crate) fn pin_mongos(&mut self, address: ServerAddress) {
        self.transaction.pinned_mongos = Some(SelectionCriteria::Predicate(Arc::new(
            move |server_info: &ServerInfo| *server_info.address() == address,
        )));
    }

    pub(crate) fn unpin_mongos(&mut self) {
        self.transaction.pinned_mongos = None;
    }

    /// Whether this session is dirty.
    #[cfg(test)]
    pub(crate) fn is_dirty(&self) -> bool {
        self.server_session.dirty
    }

    /// Starts a new transaction on this session with the given `TransactionOptions`. If no options
    /// are provided, the session's `defaultTransactionOptions` will be used. This session must
    /// be passed into each operation within the transaction; otherwise, the operation will be
    /// executed outside of the transaction.
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
    /// # let mut session = client.start_session(None).await?;
    /// session.start_transaction(None).await?;
    /// let result = coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// session.commit_transaction().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_transaction(
        &mut self,
        options: impl Into<Option<TransactionOptions>>,
    ) -> Result<()> {
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
                self.unpin_mongos(); // Unpin session if previous transaction is committed.
            }
            _ => {}
        }
        match self.client.transaction_support_status().await? {
            TransactionSupportStatus::Supported => {
                let mut options = match options.into() {
                    Some(mut options) => {
                        if let Some(defaults) = self.default_transaction_options() {
                            merge_options!(
                                defaults,
                                &mut options,
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
    /// # let mut session = client.start_session(None).await?;
    /// session.start_transaction(None).await?;
    /// let result = coll.insert_one_with_session(doc! { "x": 1 }, None, &mut session).await?;
    /// session.commit_transaction().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn commit_transaction(&mut self) -> Result<()> {
        match &mut self.transaction.state {
            TransactionState::None => Err(ErrorKind::Transaction {
                message: "no transaction started".into(),
            }
            .into()),
            TransactionState::Aborted => Err(ErrorKind::Transaction {
                message: "Cannot call commitTransaction after calling abortTransaction".into(),
            }
            .into()),
            TransactionState::Starting => {
                self.transaction.commit(false);
                Ok(())
            }
            TransactionState::InProgress => {
                let commit_transaction = CommitTransaction::new(self.transaction.options.clone());
                self.transaction.commit(true);
                self.client
                    .clone()
                    .execute_operation(commit_transaction, self)
                    .await
            }
            TransactionState::Committed {
                data_committed: true,
            } => {
                let mut commit_transaction =
                    CommitTransaction::new(self.transaction.options.clone());
                commit_transaction.update_for_retry();
                self.client
                    .clone()
                    .execute_operation(commit_transaction, self)
                    .await
            }
            TransactionState::Committed {
                data_committed: false,
            } => Ok(()),
        }
    }

    /// Aborts the transaction that is currently active on this session. Any open transaction will
    /// be aborted automatically in the `Drop` implementation of `ClientSession`.
    ///
    /// ```rust
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client, ClientSession, Collection};
    /// #
    /// # async fn do_stuff() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let coll = client.database("foo").collection::<Document>("bar");
    /// # let mut session = client.start_session(None).await?;
    /// session.start_transaction(None).await?;
    /// match execute_transaction(&coll, &mut session).await {
    ///     Ok(_) => session.commit_transaction().await?,
    ///     Err(_) => session.abort_transaction().await?,
    /// }
    /// # Ok(())
    /// # }
    ///
    /// async fn execute_transaction(coll: &Collection<Document>, session: &mut ClientSession) -> Result<()> {
    ///     coll.insert_one_with_session(doc! { "x": 1 }, None, session).await?;
    ///     coll.delete_one_with_session(doc! { "y": 2 }, None, session).await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// This operation will retry once upon failure if the connection and encountered error support
    /// retryability. See the documentation
    /// [here](https://docs.mongodb.com/manual/core/retryable-writes/) for more information on
    /// retryable writes.
    pub async fn abort_transaction(&mut self) -> Result<()> {
        match self.transaction.state {
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
                self.transaction.abort();
                Ok(())
            }
            TransactionState::InProgress => {
                let write_concern = self
                    .transaction
                    .options
                    .as_ref()
                    .and_then(|options| options.write_concern.as_ref())
                    .cloned();
                let selection_criteria = self.transaction.pinned_mongos.clone();
                let abort_transaction = AbortTransaction::new(write_concern, selection_criteria);
                self.transaction.abort();
                // Errors returned from running an abortTransaction command should be ignored.
                let _result = self
                    .client
                    .clone()
                    .execute_operation(abort_transaction, &mut *self)
                    .await;
                Ok(())
            }
        }
    }

    fn default_transaction_options(&self) -> Option<&TransactionOptions> {
        self.options
            .as_ref()
            .and_then(|options| options.default_transaction_options.as_ref())
    }
}

struct DroppedClientSession {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
    client: Client,
    is_implicit: bool,
    options: Option<SessionOptions>,
    transaction: Transaction,
    snapshot_time: Option<Timestamp>,
}

impl From<DroppedClientSession> for ClientSession {
    fn from(dropped_session: DroppedClientSession) -> Self {
        Self {
            cluster_time: dropped_session.cluster_time,
            server_session: dropped_session.server_session,
            client: dropped_session.client,
            is_implicit: dropped_session.is_implicit,
            options: dropped_session.options,
            transaction: dropped_session.transaction,
            snapshot_time: dropped_session.snapshot_time,
        }
    }
}

impl Drop for ClientSession {
    fn drop(&mut self) {
        if self.transaction.state == TransactionState::InProgress {
            let dropped_session = DroppedClientSession {
                cluster_time: self.cluster_time.clone(),
                server_session: self.server_session.clone(),
                client: self.client.clone(),
                is_implicit: self.is_implicit,
                options: self.options.clone(),
                transaction: self.transaction.clone(),
                snapshot_time: self.snapshot_time,
            };
            RUNTIME.execute(async move {
                let mut session: ClientSession = dropped_session.into();
                let _result = session.abort_transaction().await;
            });
        } else {
            let client = self.client.clone();
            let server_session = self.server_session.clone();
            RUNTIME.execute(async move {
                client.check_in_server_session(server_session).await;
            });
        }
    }
}

/// Client side abstraction of a server session. These are pooled and may be associated with
/// multiple `ClientSession`s over the course of their lifetime.
#[derive(Clone, Debug)]
pub(crate) struct ServerSession {
    /// The id of the server session to which this corresponds.
    id: Document,

    /// The last time an operation was executed with this session.
    last_use: std::time::Instant,

    /// Whether a network error was encountered while using this session.
    dirty: bool,

    /// A monotonically increasing transaction number for this session.
    txn_number: i64,
}

impl ServerSession {
    /// Creates a new session, generating the id client side.
    fn new() -> Self {
        let binary = Bson::Binary(Binary {
            subtype: BinarySubtype::Uuid,
            bytes: Uuid::new_v4().as_bytes().to_vec(),
        });

        Self {
            id: doc! { "id": binary },
            last_use: Instant::now(),
            dirty: false,
            txn_number: 0,
        }
    }

    /// Determines if this server session is about to expire in a short amount of time (1 minute).
    fn is_about_to_expire(&self, logical_session_timeout: Option<Duration>) -> bool {
        let timeout = if let Some(t) = logical_session_timeout { t } else { return false; };
        let expiration_date = self.last_use + timeout;
        expiration_date < Instant::now() + Duration::from_secs(60)
    }
}
