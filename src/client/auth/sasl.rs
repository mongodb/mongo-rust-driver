use crate::{
    bson::{doc, spec::BinarySubtype, Binary, Bson, Document},
    bson_util,
    client::{auth::AuthMechanism, options::ServerApi},
    cmap::Command,
    error::{Error, Result},
};

/// Encapsulates the command building of a `saslStart` command.
pub(super) struct SaslStart {
    source: String,
    mechanism: AuthMechanism,
    payload: Vec<u8>,
    server_api: Option<ServerApi>,
}

impl SaslStart {
    pub(super) fn new(
        source: String,
        mechanism: AuthMechanism,
        payload: Vec<u8>,
        server_api: Option<ServerApi>,
    ) -> Self {
        Self {
            source,
            mechanism,
            payload,
            server_api,
        }
    }

    pub(super) fn into_command(self) -> Command {
        let mut body = doc! {
            "saslStart": 1,
            "mechanism": self.mechanism.as_str(),
            "payload": Binary { subtype: BinarySubtype::Generic, bytes: self.payload },
        };
        if self.mechanism == AuthMechanism::ScramSha1
            || self.mechanism == AuthMechanism::ScramSha256
        {
            body.insert("options", doc! { "skipEmptyExchange": true });
        }

        let mut command = Command::new("saslStart".into(), self.source, body);
        if let Some(server_api) = self.server_api {
            command.set_server_api(&server_api);
        }

        command
    }
}

/// Encapsulates the command building of a `saslContinue` command.
pub(super) struct SaslContinue {
    source: String,
    conversation_id: Bson,
    payload: Vec<u8>,
    server_api: Option<ServerApi>,
}

impl SaslContinue {
    pub(super) fn new(
        source: String,
        conversation_id: Bson,
        payload: Vec<u8>,
        server_api: Option<ServerApi>,
    ) -> Self {
        Self {
            source,
            conversation_id,
            payload,
            server_api,
        }
    }

    pub(super) fn into_command(self) -> Command {
        let body = doc! {
            "saslContinue": 1,
            "conversationId": self.conversation_id,
            "payload": Binary { subtype: BinarySubtype::Generic, bytes: self.payload },
        };

        let mut command = Command::new("saslContinue".into(), self.source, body);
        if let Some(server_api) = self.server_api {
            command.set_server_api(&server_api);
        }

        command
    }
}

/// Validates that a `saslStart` or `saslContinue` command response is successful.
fn validate_command_success(auth_mechanism: &str, response: &Document) -> Result<()> {
    let ok = match response.get("ok") {
        Some(ok) => ok,
        None => return Ok(()),
    };

    match bson_util::get_int(ok) {
        Some(1) => Ok(()),
        Some(_) => Err(Error::authentication_error(
            auth_mechanism,
            response
                .get_str("errmsg")
                .unwrap_or("Authentication failure"),
        )),
        _ => Err(Error::invalid_authentication_response(auth_mechanism)),
    }
}

/// Encapsulates the parsing of the response to a `saslStart` or `saslContinue` command.
pub(super) struct SaslResponse {
    pub(super) conversation_id: Bson,
    pub(super) done: bool,
    pub(super) payload: Vec<u8>,
}

impl SaslResponse {
    pub(super) fn parse(auth_mechanism: &str, mut response: Document) -> Result<Self> {
        validate_command_success(auth_mechanism, &response)?;

        let conversation_id = response
            .remove("conversationId")
            .ok_or_else(|| Error::invalid_authentication_response(auth_mechanism))?;
        let done = response
            .remove("done")
            .and_then(|b| b.as_bool())
            .ok_or_else(|| Error::invalid_authentication_response(auth_mechanism))?;
        let payload = response
            .get_binary_generic_mut("payload")
            .map_err(|_| Error::invalid_authentication_response(auth_mechanism))?
            .drain(..)
            .collect();

        Ok(SaslResponse {
            conversation_id,
            done,
            payload,
        })
    }
}
