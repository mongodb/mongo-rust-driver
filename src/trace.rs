use bson::Bson;

pub(crate) mod command;

pub(crate) const COMMAND_TRACING_EVENT_TARGET: &str = "mongodb::command";

trait TracingRepresentation {
    fn tracing_representation(self) -> String;
}

impl TracingRepresentation for bson::oid::ObjectId {
    fn tracing_representation(self) -> String {
        self.to_hex()
    }
}

impl TracingRepresentation for bson::Document {
    fn tracing_representation(self) -> String {
        Bson::Document(self).into_relaxed_extjson().to_string()
    }
}

impl TracingRepresentation for crate::error::Error {
    fn tracing_representation(self) -> String {
        self.to_string()
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
