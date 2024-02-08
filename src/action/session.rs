use crate::{
    client::options::{SessionOptions, TransactionOptions},
    error::Result,
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

/// Starts a new [`ClientSession`].  Create by calling [`Client::start_session`].
#[must_use]
pub struct StartSession<'a> {
    client: &'a Client,
    options: Option<SessionOptions>,
}

impl<'a> StartSession<'a> {
    option_setters!(options: SessionOptions;
        default_transaction_options: TransactionOptions,
        causal_consistency: bool,
        snapshot: bool,
    );
}

action_impl! {
    impl<'a> Action for StartSession<'a> {
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
