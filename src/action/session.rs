use serde::Deserialize;
use typed_builder::TypedBuilder;

use crate::{
    client::options::TransactionOptions,
    error::{ErrorKind, Result},
    Client,
    ClientSession,
};

use super::{action_impl, option_setters};

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

/// Contains the options that can be used to create a new [`ClientSession`].
#[derive(Clone, Debug, Default, Deserialize, TypedBuilder)]
#[builder(field_defaults(default, setter(into)))]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct SessionOptions {
    /// The default options to use for transactions started on this session.
    ///
    /// If these options are not specified, they will be inherited from the
    /// [`Client`](../struct.Client.html) associated with this session. They will not
    /// be inherited from the options specified
    /// on the [`Database`](../struct.Database.html) or [`Collection`](../struct.Collection.html)
    /// associated with the operations within the transaction.
    pub default_transaction_options: Option<TransactionOptions>,

    /// If true, all operations performed in the context of this session
    /// will be [causally consistent](https://www.mongodb.com/docs/manual/core/causal-consistency-read-write-concerns/).
    ///
    /// Defaults to true if [`SessionOptions::snapshot`] is unspecified.
    pub causal_consistency: Option<bool>,

    /// If true, all read operations performed using this client session will share the same
    /// snapshot.  Defaults to false.
    pub snapshot: Option<bool>,
}

impl<'a> StartSession<'a> {
    option_setters!(options: SessionOptions;
        default_transaction_options: TransactionOptions,
        causal_consistency: bool,
        snapshot: bool,
    );
}

impl SessionOptions {
    pub(crate) fn validate(&self) -> Result<()> {
        if let (Some(causal_consistency), Some(snapshot)) = (self.causal_consistency, self.snapshot)
        {
            if causal_consistency && snapshot {
                return Err(ErrorKind::InvalidArgument {
                    message: "snapshot and causal consistency are mutually exclusive".to_string(),
                }
                .into());
            }
        }
        Ok(())
    }
}

action_impl! {
    impl Action<'a> for StartSession<'a> {
        type Future = StartSessionFuture;

        async fn execute(self) -> Result<ClientSession> {
            if let Some(options) = &self.options {
                options.validate()?;
            }
            Ok(ClientSession::new(self.client.clone(), self.options, false).await)
        }

        fn sync_wrap(out) -> Result<crate::sync::ClientSession> {
            out.map(Into::into)
        }
    }
}
