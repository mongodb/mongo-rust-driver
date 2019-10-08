use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use super::wire::Message;
use crate::{bson_util, concern::WriteConcern, error::Result, read_preference::ReadPreference};

/// `Command` is a driver side abstraction of a server command containing all the information
/// necessary to serialize it to a wire message.
pub(crate) struct Command {
    pub(crate) name: String,
    pub(crate) target_db: String,
    pub(crate) read_pref: Option<ReadPreference>,
    pub(crate) write_concern: Option<WriteConcern>,
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
            write_concern: None,
            body,
        }
    }

    /// Constructs a write command.
    pub(crate) fn new_write(
        name: String,
        target_db: String,
        write_concern: Option<WriteConcern>,
        body: Document,
    ) -> Self {
        Self {
            name,
            target_db,
            read_pref: None,
            write_concern,
            body,
        }
    }

    /// Constructs a command that may both read and write (e.g. findAndModify).
    pub(crate) fn new_read_write(
        name: String,
        target_db: String,
        read_pref: Option<ReadPreference>,
        write_concern: Option<WriteConcern>,
        body: Document,
    ) -> Self {
        Self {
            name,
            target_db,
            read_pref,
            write_concern,
            body,
        }
    }
}

pub(crate) struct CommandResponse {
    raw_response: Document,
}

impl CommandResponse {
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

    /// Deserialize the body of the response.
    pub(crate) fn body<T: DeserializeOwned>(&self) -> Result<T> {
        let body = bson::from_bson(Bson::Document(self.raw_response.clone()))?;
        Ok(body)
    }

    /// The raw server response.
    pub(crate) fn raw(&self) -> &Document {
        &self.raw_response
    }
}
