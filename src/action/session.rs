use crate::{
    client::options::{SessionOptions, TransactionOptions},
    error::Result,
    Client,
    ClientSession,
};

use super::{action_impl, deeplink, option_setters};

impl Client {
    /// Starts a new [`ClientSession`].
    ///
    /// `await` will return d[`Result<ClientSession>`].
    #[deeplink]
    pub fn start_session(&self) -> StartSession {
        StartSession {
            client: self,
            options: None,
        }
    }
}

#[cfg(feature = "sync")]
impl crate::sync::Client {
    /// Starts a new [`ClientSession`].
    ///
    /// [run](StartSession::run) will return d[`Result<crate::sync::ClientSession>`].
    #[deeplink]
    pub fn start_session(&self) -> StartSession {
        self.async_client.start_session()
    }
}

/// Starts a new [`ClientSession`].  Construct with [`Client::start_session`].
#[must_use]
pub struct StartSession<'a> {
    client: &'a Client,
    options: Option<SessionOptions>,
}

impl StartSession<'_> {
    option_setters!(options: SessionOptions;
        default_transaction_options: TransactionOptions,
        causal_consistency: bool,
        snapshot: bool,
    );
}

#[action_impl(sync = crate::sync::ClientSession)]
impl<'a> Action for StartSession<'a> {
    type Future = StartSessionFuture;

    async fn execute(self) -> Result<ClientSession> {
        if let Some(options) = &self.options {
            options.validate()?;
        }
        Ok(ClientSession::new(self.client.clone(), self.options, false).await)
    }
}
