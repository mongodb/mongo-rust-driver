use crate::{
    client::options::{SessionOptions, TransactionOptions},
    error::Result,
    Client,
    ClientSession,
};

use super::{action_execute, option_setters};

impl Client {
    /// Starts a new [`ClientSession`].
    ///
    /// `await` will return `Result<`[`ClientSession`]`>`.
    pub fn start_session(&self) -> StartSession {
        StartSession {
            client: self,
            options: None,
        }
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl crate::sync::Client {
    /// Starts a new [`ClientSession`].
    ///
    /// [run](StartSession::run) will return `Result<`[`ClientSession`]`>`.
    pub fn start_session(&self) -> StartSession {
        self.async_client.start_session()
    }
}

/// Starts a new [`ClientSession`].  Create by calling [`Client::start_session`] and execute with
/// `await` (or [run](StartSession::run) if using the sync client).
#[must_use]
pub struct StartSession<'a> {
    client: &'a Client,
    options: Option<SessionOptions>,
}

impl<'a> StartSession<'a> {
    option_setters!(options: SessionOptions;
        /// The default options to use for transactions started on this session.
        ///
        /// If these options are not specified, they will be inherited from the
        /// [`Client`](../struct.Client.html) associated with this session. They will not
        /// be inherited from the options specified
        /// on the [`Database`](../struct.Database.html) or [`Collection`](../struct.Collection.html)
        /// associated with the operations within the transaction.
        default_transaction_options: TransactionOptions,

        /// If true, all operations performed in the context of this session
        /// will be [causally consistent](https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/).
        ///
        /// Defaults to true if [`snapshot`](StartSession::snapshot) is unspecified.
        causal_consistency: bool,

        /// If true, all read operations performed using this client session will share the same
        /// snapshot.  Defaults to false.
        snapshot: bool,
    );
}

action_execute! {
    StartSession<'a> => StartSessionFuture;

    async fn(self) -> Result<ClientSession> {
        if let Some(options) = &self.options {
            options.validate()?;
        }
        Ok(ClientSession::new(self.client.clone(), self.options, false).await)
    }
}

#[cfg(any(feature = "sync", feature = "tokio-sync"))]
impl<'a> StartSession<'a> {
    /// Synchronously execute this action.
    pub fn run(self) -> Result<crate::sync::ClientSession> {
        crate::runtime::block_on(self.into_future()).map(Into::into)
    }
}
