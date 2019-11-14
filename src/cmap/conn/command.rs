use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use super::wire::Message;
use crate::{
    bson_util, error::CommandError, error::ErrorKind, error::Result,
    selection_criteria::ReadPreference,
};

/// `Command` is a driver side abstraction of a server command containing all the information
/// necessary to serialize it to a wire message.
#[derive(Debug, Clone)]
pub(crate) struct Command {
    pub(crate) name: String,
    pub(crate) target_db: String,
    pub(crate) read_pref: Option<ReadPreference>,
    pub(crate) body: Document,
}

impl Command {
    /// Constructs a read command.
    pub(crate) fn new_read(
        name: String,
        target_db: String,
        read_pref: Option<ReadPreference>,
        body: Document,
    ) -> Self {
        Self {
            name,
            target_db,
            read_pref,
            body,
        }
    }

    /// Constructs a new command.
    pub(crate) fn new(name: String, target_db: String, body: Document) -> Self {
        Self {
            name,
            target_db,
            read_pref: None,
            body,
        }
    }
}

pub(crate) struct CommandResponse {
    pub(crate) raw_response: Document,
}

impl CommandResponse {
    /// Initialize a resposne from a document.
    /// This should be used for test purposes only.
    pub(crate) fn from_document(doc: Document) -> Self {
        Self { raw_response: doc }
    }

    pub(crate) fn from_message(message: Message) -> Result<Self> {
        Ok(Self {
            raw_response: message.single_document_response()?,
        })
    }

    /// Returns whether this response indicates a success or not (i.e. if "ok: 1")
    pub(crate) fn is_success(&self) -> bool {
        match self.raw_response.get("ok") {
            Some(ref b) => bson_util::get_int(b) == Some(1),
            _ => false,
        }
    }

    /// Retunrs a result indicating whether this response corresponds to a command failure.
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.is_success() {
            let command_error: CommandError =
                bson::from_bson(Bson::Document(self.raw_response.clone())).map_err(|_| {
                    ErrorKind::ResponseError {
                        message: "invalid server response".to_string(),
                    }
                })?;
            Err(ErrorKind::CommandError(command_error).into())
        } else {
            Ok(())
        }
    }

    /// Deserialize the body of the response.
    /// If this response corresponds to a command failure, an appropriate CommandError result will
    /// be returned.
    pub(crate) fn body<T: DeserializeOwned>(&self) -> Result<T> {
        self.validate()?;
        let body = bson::from_bson(Bson::Document(self.raw_response.clone()))?;
        Ok(body)
    }

    /// The raw server response.
    pub(crate) fn raw(&self) -> &Document {
        &self.raw_response
    }
}
