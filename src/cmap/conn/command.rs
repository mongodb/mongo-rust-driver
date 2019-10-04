use bson::{Bson, Document};
use serde::de::DeserializeOwned;

use super::wire::Message;
use crate::{bson_util, concern::WriteConcern, error::Result, read_preference::ReadPreference};

/// `Command` is a driver side abstraction of a server command containing all the information
/// necessary to serialize it to a wire message.
pub(crate) struct Command {
    name: &'static str,
    target_db: String,
    read_pref: Option<ReadPreference>,
    write_concern: Option<WriteConcern>,
    body: Document,
}

impl Command {
    /// Constructs a read command.
    pub(crate) fn new_read(
        name: &'static str,
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
        name: &'static str,
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
        name: &'static str,
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

    pub(crate) fn name(&self) -> &'static str {
        self.name
    }

    pub(crate) fn target_db(&self) -> &str {
        self.target_db.as_str()
    }

    pub(crate) fn read_preference(&self) -> Option<ReadPreference> {
        self.read_pref.clone()
    }

    pub(crate) fn write_concern(&self) -> Option<WriteConcern> {
        self.write_concern.clone()
    }

    pub(crate) fn body(&self) -> Document {
        self.body.clone()
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
        match bson::from_bson(Bson::Document(self.raw_response.clone())) {
            Ok(r) => Ok(r),
            Err(e) => Err(e.into()),
        }
    }

    /// The raw server response.
    pub(crate) fn raw(&self) -> &Document {
        &self.raw_response
    }
}
