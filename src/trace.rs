use crate::{
    bson::Bson,
    client::options::{ServerAddress, DEFAULT_PORT},
};

pub(crate) mod command;
pub(crate) mod connection;
pub(crate) mod server_selection;
pub(crate) mod topology;

pub(crate) const COMMAND_TRACING_EVENT_TARGET: &str = "mongodb::command";
pub(crate) const CONNECTION_TRACING_EVENT_TARGET: &str = "mongodb::connection";
pub(crate) const SERVER_SELECTION_TRACING_EVENT_TARGET: &str = "mongodb::server_selection";
pub(crate) const TOPOLOGY_TRACING_EVENT_TARGET: &str = "mongodb::topology";

pub(crate) const DEFAULT_MAX_DOCUMENT_LENGTH_BYTES: usize = 1000;

pub(crate) trait TracingRepresentation {
    type Representation;

    fn tracing_representation(&self) -> Self::Representation;
}

impl TracingRepresentation for bson::oid::ObjectId {
    type Representation = String;

    fn tracing_representation(&self) -> String {
        self.to_hex()
    }
}

impl TracingRepresentation for crate::error::Error {
    type Representation = String;

    fn tracing_representation(&self) -> String {
        self.to_string()
    }
}

impl ServerAddress {
    /// Per spec should populate the port field with 27017 if we are defaulting to that.
    pub(crate) fn port_tracing_representation(&self) -> Option<u16> {
        match self {
            Self::Tcp { port, .. } => Some(port.unwrap_or(DEFAULT_PORT)),
            // For Unix domain sockets we should return None here, as ports
            // are not meaningful for those.
            #[cfg(unix)]
            Self::Unix { .. } => None,
        }
    }
}

/// Truncates the given string at the closest UTF-8 character boundary >= the provided length.
/// If the new length is >= the current length, does nothing.
pub(crate) fn truncate_on_char_boundary(s: &mut String, new_len: usize) {
    let original_len = s.len();
    if original_len > new_len {
        // to avoid generating invalid UTF-8, find the first index >= max_length_bytes that is
        // the end of a character.
        // TODO: RUST-1496 we should use ceil_char_boundary here but it's currently nightly-only.
        // see: https://doc.rust-lang.org/std/string/struct.String.html#method.ceil_char_boundary
        let mut truncate_index = new_len;
        // is_char_boundary returns true when the provided value == the length of the string, so
        // if we reach the end of the string this loop will terminate.
        while !s.is_char_boundary(truncate_index) {
            truncate_index += 1;
        }
        s.truncate(truncate_index);
        // due to the "rounding up" behavior we might not actually end up truncating anything.
        // if we did, spec requires we add a trailing "...".
        if truncate_index < original_len {
            s.push_str("...")
        }
    }
}

fn serialize_command_or_reply(doc: bson::Document, max_length_bytes: usize) -> String {
    let mut ext_json = Bson::Document(doc).into_relaxed_extjson().to_string();
    truncate_on_char_boundary(&mut ext_json, max_length_bytes);
    ext_json
}

/// We don't currently use all of these levels but they are included for completeness.
#[allow(dead_code)]
pub(crate) enum TracingOrLogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl TracingOrLogLevel {
    pub(crate) const fn as_log_level(&self) -> log::Level {
        match self {
            TracingOrLogLevel::Error => log::Level::Error,
            TracingOrLogLevel::Warn => log::Level::Warn,
            TracingOrLogLevel::Info => log::Level::Info,
            TracingOrLogLevel::Debug => log::Level::Debug,
            TracingOrLogLevel::Trace => log::Level::Trace,
        }
    }

    pub(crate) const fn as_tracing_level(&self) -> tracing::Level {
        match self {
            TracingOrLogLevel::Error => tracing::Level::ERROR,
            TracingOrLogLevel::Warn => tracing::Level::WARN,
            TracingOrLogLevel::Info => tracing::Level::INFO,
            TracingOrLogLevel::Debug => tracing::Level::DEBUG,
            TracingOrLogLevel::Trace => tracing::Level::TRACE,
        }
    }
}

/// Pending https://github.com/tokio-rs/tracing/issues/2036 we can remove this and just use tracing::enabled.
macro_rules! trace_or_log_enabled {
    (target: $target:expr, $lvl:expr) => {
        tracing::enabled!(target: $target, $lvl.as_tracing_level())
            || log::log_enabled!(target: $target, $lvl.as_log_level())
    };
}
pub(crate) use trace_or_log_enabled;
