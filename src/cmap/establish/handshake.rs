use bson::{bson, doc, Document};
use lazy_static::lazy_static;
use os_info::{Type, Version};

use crate::{
    cmap::{conn::command::Command, options::ConnectionPoolOptions, Connection, StreamDescription},
    error::Result,
    is_master::IsMasterReply,
};

lazy_static! {
    /// Contains the basic handshake information that can be statically determined. This document
    /// (potentially with additional fields added) can be cloned and put in the `client` field of
    /// the `isMaster` command.
    static ref BASE_HANDSHAKE_DOCUMENT: Document = {
        let mut doc = doc! {
            "driver": {
                "name": "mongo-rust-driver",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "os": {
                "type": std::env::consts::OS,
                "architecture": std::env::consts::ARCH,
            },
        };

        let os_subdoc = doc.get_document_mut("os").unwrap();

        let info = os_info::get();

        if info.os_type() != Type::Unknown {
            let version = info.version();

            if *version != Version::unknown() {
                os_subdoc.insert("version", info.version().to_string());
            }
        }

        if let Some((version, channel, date)) = version_check::triple() {
            os_subdoc.insert(
                "platform",
                format!("rustc {} {} ({})", version, channel, date),
            );
        }

        doc
    };
}

/// Contains the logic needed to handshake a connection.
#[derive(Debug, Clone)]
pub(super) struct Handshaker {
    /// The `isMaster` command to send when handshaking. This will always be identical
    /// given the same pool options, so it can be created at the time the Handshaker is created.
    command: Command,
}

impl Handshaker {
    /// Creates a new Handshaker.
    pub(super) fn new(options: Option<&ConnectionPoolOptions>) -> Self {
        let mut document = BASE_HANDSHAKE_DOCUMENT.clone();

        if let Some(ref app_name) = options.as_ref().and_then(|opts| opts.app_name.as_ref()) {
            document.insert("application", doc! { "name": app_name.to_string() });
        }

        // TODO RUST-204: Add "saslSupportedMechs" if applicable.

        // TODO RUST-204: Choose proper database to handshake against.
        let db = "admin";

        let body = doc! {
                "isMaster": 1,
                "client": document,
        };

        Self {
            command: Command::new_read("isMaster".to_string(), db.to_string(), None, body),
        }
    }

    /// Handshakes a connection.
    pub(super) fn handshake<'a>(
        &'a self,
        conn: &'a mut Connection,
    ) -> Result<&'a StreamDescription> {
        let response = conn.send_command(self.command.clone())?;
        let command_response = response.body()?;

        // TODO RUST-192: Calculate round trip time.
        let is_master_reply = IsMasterReply {
            command_response,
            round_trip_time: None,
        };

        conn.stream_description = Some(StreamDescription::from_is_master(is_master_reply));
        conn.stream_description().ok_or_else(|| unreachable!())
    }
}
