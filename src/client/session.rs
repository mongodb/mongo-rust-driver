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
    cmap::conn::PinnedConnectionHandle,
    error::{ErrorKind, Result},
    operation::{AbortTransaction, CommitTransaction, Operation},
    options::{SessionOptions, TransactionOptions},
    sdam::{ServerInfo, TransactionSupportStatus},
    selection_criteria::SelectionCriteria,
    BoxFuture,
    Client,
};
pub use cluster_time::ClusterTime;
pub(super) use pool::ServerSessionPool;

use super::{options::ServerAddress, AsyncDropToken};

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
/// see the [manual](https://www.mongodb.com/docs/manual/core/transactions/).
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
#[derive(Debug)]
pub struct ClientSession {
    cluster_time: Option<ClusterTime>,
    server_session: ServerSession,
    client: Client,
    is_implicit: bool,
    options: Option<SessionOptions>,
    drop_token: AsyncDropToken,
    pub(crate) transaction: Transaction,
    pub(crate) snapshot_time: Option<Timestamp>,
    pub(crate) operation_time: Option<Timestamp>,
    #[cfg(test)]
    pub(crate) convenient_transaction_timeout: Option<Duration>,
}

#[derive(Debug)]
pub(crate) struct Transaction {
    pub(crate) state: TransactionState,
    pub(crate) options: Option<TransactionOptions>,
    pub(crate) pinned: Option<TransactionPin>,
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
        self.pinned = None;
    }

    pub(crate) fn reset(&mut self) {
        self.state = TransactionState::None;
        self.options = None;
        self.pinned = None;
        self.recovery_token = None;
    }

    pub(crate) fn pinned_mongos(&self) -> Option<&SelectionCriteria> {
        match &self.pinned {
            Some(TransactionPin::Mongos(s)) => Some(s),
            _ => None,
        }
    }

    pub(crate) fn pinned_connection(&self) -> Option<&PinnedConnectionHandle> {
        match &self.pinned {
            Some(TransactionPin::Connection(c)) => Some(c),
            _ => None,
        }
    }

    fn take(&mut self) -> Self {
        Transaction {
            state: self.state.clone(),
            options: self.options.take(),
            pinned: self.pinned.take(),
            recovery_token: self.recovery_token.take(),
        }
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            state: TransactionState::None,
            options: None,
            pinned: None,
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

#[derive(Debug)]
pub(crate) enum TransactionPin {
    Mongos(SelectionCriteria),
    Connection(PinnedConnectionHandle),
}

impl ClientSession {
    /// Creates a new `ClientSession` by checking out a corresponding `ServerSession` from the
    /// provided client's session pool.
    pub(crate) async fn new(
        client: Client,
        options: Option<SessionOptions>,
        is_implicit: bool,
    ) -> Self {
        let timeout = client.inner.topology.logical_session_timeout();
        let server_session = client.inner.session_pool.check_out(timeout).await;
        Self {
            drop_token: client.register_async_drop(),
            client,
            server_session,
            cluster_time: None,
            is_implicit,
            options,
            transaction: Default::default(),
            snapshot_time: None,
            operation_time: None,
            #[cfg(test)]
            convenient_transaction_timeout: None,
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
    pub(crate) fn options(&self) -> Option<&SessionOptions> {
        self.options.as_ref()
    }

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub fn advance_cluster_time(&mut self, to: &ClusterTime) {
        if self.cluster_time().map(|ct| ct < to).unwrap_or(true) {
            self.cluster_time = Some(to.clone());
        }
    }

    /// Advance operation time for this session. If the provided timestamp is earlier than this
    /// session's current operation time, then the operation time is unchanged.
    pub fn advance_operation_time(&mut self, ts: Timestamp) {
        self.operation_time = match self.operation_time {
            Some(current_op_time) if current_op_time < ts => Some(ts),
            None => Some(ts),
            _ => self.operation_time,
        }
    }

    /// The operation time returned by the last operation executed in this session.
    pub fn operation_time(&self) -> Option<Timestamp> {
        self.operation_time
    }

    pub(crate) fn causal_consistency(&self) -> bool {
        self.options()
            .and_then(|opts| opts.causal_consistency)
            .unwrap_or(!self.is_implicit())
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
        self.transaction.pinned = Some(TransactionPin::Mongos(SelectionCriteria::Predicate(
            Arc::new(move |server_info: &ServerInfo| *server_info.address() == address),
        )));
    }

    /// Pin the connection to the session.
    pub(crate) fn pin_connection(&mut self, handle: PinnedConnectionHandle) {
        self.transaction.pinned = Some(TransactionPin::Connection(handle));
    }

    pub(crate) fn unpin(&mut self) {
        self.transaction.pinned = None;
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
                self.unpin(); // Unpin session if previous transaction is committed.
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
    /// [here](https://www.mongodb.com/docs/manual/core/retryable-writes/) for more information on
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
                let abort_transaction =
                    AbortTransaction::new(write_concern, self.transaction.pinned.take());
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
    /// convenience, `with_transaction` accepts a context argument that will be passed to the
    /// callback along with the session:
    ///
    /// ```no_run
    /// # use mongodb::{bson::{doc, Document}, error::Result, Client};
    /// # use futures::FutureExt;
    /// # async fn wrapper() -> Result<()> {
    /// # let client = Client::with_uri_str("mongodb://example.com").await?;
    /// # let mut session = client.start_session(None).await?;
    /// let coll = client.database("mydb").collection::<Document>("mycoll");
    /// let my_data = "my data".to_string();
    /// // This works:
    /// session.with_transaction(
    ///     (&coll, &my_data),
    ///     |session, (coll, my_data)| async move {
    ///         coll.insert_one_with_session(doc! { "data": *my_data }, None, session).await
    ///     }.boxed(),
    ///     None,
    /// ).await?;
    /// /* This will not compile with a "variable moved due to use in generator" error:
    /// session.with_transaction(
    ///     (),
    ///     |session, _| async move {
    ///         coll.insert_one_with_session(doc! { "data": my_data }, None, session).await
    ///     }.boxed(),
    ///     None,
    /// ).await?;
    /// */
    /// # Ok(())
    /// # }
    /// ```
    pub async fn with_transaction<R, C, F>(
        &mut self,
        mut context: C,
        mut callback: F,
        options: impl Into<Option<TransactionOptions>>,
    ) -> Result<R>
    where
        F: for<'a> FnMut(&'a mut ClientSession, &'a mut C) -> BoxFuture<'a, Result<R>>,
    {
        let options = options.into();
        let timeout = Duration::from_secs(120);
        #[cfg(test)]
        let timeout = self.convenient_transaction_timeout.unwrap_or(timeout);
        let start = Instant::now();

        use crate::error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT};

        'transaction: loop {
            self.start_transaction(options.clone()).await?;
            let ret = match callback(self, &mut context).await {
                Ok(v) => v,
                Err(e) => {
                    if matches!(
                        self.transaction.state,
                        TransactionState::Starting | TransactionState::InProgress
                    ) {
                        self.abort_transaction().await?;
                    }
                    if e.contains_label(TRANSIENT_TRANSACTION_ERROR) && start.elapsed() < timeout {
                        continue 'transaction;
                    }
                    return Err(e);
                }
            };
            if matches!(
                self.transaction.state,
                TransactionState::None
                    | TransactionState::Aborted
                    | TransactionState::Committed { .. }
            ) {
                return Ok(ret);
            }
            'commit: loop {
                match self.commit_transaction().await {
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
    operation_time: Option<Timestamp>,
}

impl From<DroppedClientSession> for ClientSession {
    fn from(dropped_session: DroppedClientSession) -> Self {
        Self {
            cluster_time: dropped_session.cluster_time,
            server_session: dropped_session.server_session,
            drop_token: dropped_session.client.register_async_drop(),
            client: dropped_session.client,
            is_implicit: dropped_session.is_implicit,
            options: dropped_session.options,
            transaction: dropped_session.transaction,
            snapshot_time: dropped_session.snapshot_time,
            operation_time: dropped_session.operation_time,
            #[cfg(test)]
            convenient_transaction_timeout: None,
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
                transaction: self.transaction.take(),
                snapshot_time: self.snapshot_time,
                operation_time: self.operation_time,
            };
            self.drop_token.spawn(async move {
                let mut session: ClientSession = dropped_session.into();
                let _result = session.abort_transaction().await;
            });
        } else {
            let client = self.client.clone();
            let server_session = self.server_session.clone();
            self.drop_token.spawn(async move {
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
        let timeout = match logical_session_timeout {
            Some(t) => t,
            None => return false,
        };
        let expiration_date = self.last_use + timeout;
        expiration_date < Instant::now() + Duration::from_secs(60)
    }
}
