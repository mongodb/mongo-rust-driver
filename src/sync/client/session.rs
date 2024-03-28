use super::Client;
use crate::{
    bson::Document,
    client::session::ClusterTime,
    error::Result,
    options::TransactionOptions,
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

impl<'a> From<&'a mut ClientSession> for &'a mut AsyncClientSession {
    fn from(value: &'a mut ClientSession) -> &'a mut AsyncClientSession {
        &mut value.async_client_session
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

    /// Set the cluster time to the provided one if it is greater than this session's highest seen
    /// cluster time or if this session's cluster time is `None`.
    pub fn advance_cluster_time(&mut self, to: &ClusterTime) {
        self.async_client_session.advance_cluster_time(to)
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
    pub fn with_transaction<R, F>(
        &mut self,
        mut callback: F,
        options: impl Into<Option<TransactionOptions>>,
    ) -> Result<R>
    where
        F: for<'a> FnMut(&'a mut ClientSession) -> Result<R>,
    {
        let options = options.into();
        let timeout = std::time::Duration::from_secs(120);
        let start = std::time::Instant::now();

        use crate::{
            client::session::TransactionState,
            error::{TRANSIENT_TRANSACTION_ERROR, UNKNOWN_TRANSACTION_COMMIT_RESULT},
        };

        'transaction: loop {
            self.start_transaction()
                .with_options(options.clone())
                .run()?;
            let ret = match callback(self) {
                Ok(v) => v,
                Err(e) => {
                    if matches!(
                        self.async_client_session.transaction.state,
                        TransactionState::Starting | TransactionState::InProgress
                    ) {
                        self.abort_transaction().run()?;
                    }
                    if e.contains_label(TRANSIENT_TRANSACTION_ERROR) && start.elapsed() < timeout {
                        continue 'transaction;
                    }
                    return Err(e);
                }
            };
            if matches!(
                self.async_client_session.transaction.state,
                TransactionState::None
                    | TransactionState::Aborted
                    | TransactionState::Committed { .. }
            ) {
                return Ok(ret);
            }
            'commit: loop {
                match self.commit_transaction().run() {
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
}
