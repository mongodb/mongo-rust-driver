use crate::{
    client::options::{ServerAddress, DEFAULT_PORT},
    sdam::TopologyDescription,
    selection_criteria::SelectionCriteria,
};
use bson::Bson;

pub(crate) mod command;
pub(crate) mod connection;

pub(crate) const COMMAND_TRACING_EVENT_TARGET: &str = "mongodb::command";
pub(crate) const CONNECTION_TRACING_EVENT_TARGET: &str = "mongodb::connection";
pub(crate) const SERVER_SELECTION_TRACING_EVENT_TARGET: &str = "mongodb::server_selection";

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

impl TracingRepresentation for bson::Document {
    type Representation = String;

    fn tracing_representation(&self) -> String {
        Bson::Document(self.clone())
            .into_relaxed_extjson()
            .to_string()
    }
}

impl TracingRepresentation for crate::error::Error {
    type Representation = String;

    fn tracing_representation(&self) -> String {
        self.to_string()
    }
}

impl TracingRepresentation for SelectionCriteria {
    type Representation = String;

    fn tracing_representation(&self) -> Self::Representation {
        self.to_string()
    }
}

impl TracingRepresentation for TopologyDescription {
    type Representation = String;

    fn tracing_representation(&self) -> Self::Representation {
        self.to_string()
    }
}

impl ServerAddress {
    /// Per spec should populate the port field with 27017 if we are defaulting to that.
    pub(crate) fn port_tracing_representation(&self) -> Option<u16> {
        match self {
            Self::Tcp { port, .. } => Some(port.unwrap_or(DEFAULT_PORT)),
            // TODO: RUST-802 For Unix domain sockets we should return None here, as ports
            // are not meaningful for those.
        }
    }
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
