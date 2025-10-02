//! Support for OpenTelemetry.

use std::sync::LazyLock;

use opentelemetry::{
    global::BoxedTracer,
    trace::{Span as _, SpanKind, Tracer as _},
    KeyValue,
};

use crate::{
    bson::Bson,
    cmap::{conn::wire::Message, Command, ConnectionInfo},
    error::{ErrorKind, Result},
    operation::Operation,
    options::{ClientOptions, ServerAddress, DEFAULT_PORT},
    Client,
};

/// Configuration for OpenTelemetry.
#[derive(Debug, Clone, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Options {
    /// Enables or disables OpenTelemtry for this client instance.  If unset, will use the value of
    /// the `OTEL_RUST_INSTRUMENTATION_MONGODB_ENABLED` environment variable.
    pub enabled: Option<bool>,
    /// Maximum length of the `db.query.text` attribute of command spans.  If unset, will use the
    /// value of the `OTEL_RUST_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH` environment
    /// variable.
    pub query_text_max_length: Option<usize>,
}

impl ClientOptions {
    fn otel_enabled(&self) -> bool {
        static ENABLED_ENV: LazyLock<bool> = LazyLock::new(|| {
            match std::env::var("OTEL_RUST_INSTRUMENTATION_MONGODB_ENABLED").as_deref() {
                Ok("1" | "true" | "yes") => true,
                _ => false,
            }
        });

        self.tracing
            .as_ref()
            .and_then(|t| t.enabled)
            .unwrap_or_else(|| *ENABLED_ENV)
    }

    fn otel_query_text_max_length(&self) -> usize {
        static MAX_LENGTH_ENV: LazyLock<usize> = LazyLock::new(|| {
            std::env::var("OTEL_RUST_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(0)
        });

        self.tracing
            .as_ref()
            .and_then(|t| t.query_text_max_length)
            .unwrap_or_else(|| *MAX_LENGTH_ENV)
    }
}

static TRACER: LazyLock<BoxedTracer> = LazyLock::new(|| {
    opentelemetry::global::tracer_with_scope(
        opentelemetry::InstrumentationScope::builder("mongodb")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    )
});

impl Client {
    pub(crate) fn start_operation_span(&self, op: &impl Operation) -> Span {
        if !self.options().otel_enabled() {
            return Span { inner: None };
        }
        let span_name = format!("{} {}", op.name(), op_target(op));
        let mut attrs = common_attrs(op);
        attrs.extend([
            KeyValue::new("db.operation.name", op.name().as_str().to_owned()),
            KeyValue::new("db.operation.summary", span_name.clone()),
        ]);
        Span {
            inner: Some(
                TRACER
                    .span_builder(span_name)
                    .with_kind(SpanKind::Client)
                    .with_attributes(attrs)
                    .start(&*TRACER),
            ),
        }
    }

    pub(crate) fn start_command_span(
        &self,
        op: &impl Operation,
        conn_info: &ConnectionInfo,
        message: &Message,
        cmd_attrs: CommandAttributes,
    ) -> Span {
        if !self.options().otel_enabled() || cmd_attrs.should_redact {
            return Span { inner: None };
        }
        let otel_driver_conn_id: i64 = conn_info.id.into();
        let mut attrs = common_attrs(op);
        attrs.extend(cmd_attrs.attrs);
        attrs.extend([
            KeyValue::new(
                "db.query.summary",
                format!("{} {}", &cmd_attrs.name, op_target(op)),
            ),
            KeyValue::new("db.mongodb.driver_connection_id", otel_driver_conn_id),
        ]);
        match &conn_info.address {
            ServerAddress::Tcp { host, port } => {
                let otel_port: i64 = port.unwrap_or(DEFAULT_PORT).into();
                attrs.extend([
                    KeyValue::new("server.port", otel_port),
                    KeyValue::new("server.address", host.clone()),
                    KeyValue::new("network.transport", "tcp"),
                ]);
            }
            ServerAddress::Unix { path } => {
                attrs.extend([
                    KeyValue::new("server.address", path.to_string_lossy().into_owned()),
                    KeyValue::new("network.transport", "unix"),
                ]);
            }
        }
        if let Some(server_id) = &conn_info.server_id {
            attrs.push(KeyValue::new("db.mongodb.server_connection_id", *server_id));
        }
        let text_max_len = self.options().otel_query_text_max_length();
        if text_max_len > 0 {
            attrs.push(KeyValue::new(
                "db.query.text",
                crate::bson_util::doc_to_json_str(message.get_command_document(), text_max_len),
            ));
        }
        if let Some(cursor_id) = op.cursor_id() {
            attrs.push(KeyValue::new("db.mongodb.cursor_id", cursor_id));
        }
        Span {
            inner: Some(
                TRACER
                    .span_builder(cmd_attrs.name)
                    .with_kind(SpanKind::Client)
                    .with_attributes(attrs)
                    .start(&*TRACER),
            ),
        }
    }
}

pub(crate) struct Span {
    inner: Option<<BoxedTracer as opentelemetry::trace::Tracer>::Span>,
}

impl Span {
    pub(crate) fn record_error<T>(&mut self, result: &Result<T>) {
        if let (Some(inner), Err(error)) = (&mut self.inner, result) {
            inner.set_attributes([
                KeyValue::new("exception.message", error.to_string()),
                KeyValue::new("exception.type", error.kind.name()),
                #[cfg(test)]
                KeyValue::new("exception.stacktrace", error.bt.to_string()),
            ]);
            if let ErrorKind::Command(cmd_err) = &*error.kind {
                inner.set_attribute(KeyValue::new(
                    "db.response.status_code",
                    cmd_err.code_name.clone(),
                ));
            }
            inner.record_error(error);
            inner.set_status(opentelemetry::trace::Status::Error {
                description: error.to_string().into(),
            });
        }
    }

    pub(crate) fn record_command_result<Op: Operation>(&mut self, result: &Result<Op::O>) {
        if let (Some(inner), Ok(out)) = (&mut self.inner, result) {
            if let Some(cursor_id) = Op::output_cursor_id(out) {
                inner.set_attribute(KeyValue::new("db.mongodb.cursor_id", cursor_id));
            }
        }
        self.record_error(result);
    }
}

fn op_target(op: &impl Operation) -> String {
    if let Some(coll) = op.collection() {
        format!("{}.{}", op.database(), coll)
    } else {
        op.database().to_owned()
    }
}

fn common_attrs(op: &impl Operation) -> Vec<KeyValue> {
    let mut attrs = vec![
        KeyValue::new("db.system", "mongodb"),
        KeyValue::new("db.namespace", op.database().to_owned()),
    ];
    if let Some(coll) = op.collection() {
        attrs.push(KeyValue::new("db.collection.name", coll.to_owned()));
    }
    attrs
}

#[derive(Clone)]
pub(crate) struct CommandAttributes {
    should_redact: bool,
    name: String,
    attrs: Vec<KeyValue>,
}

impl CommandAttributes {
    pub(crate) fn new(cmd: &Command) -> Self {
        let mut attrs = vec![KeyValue::new("db.command.name", cmd.name.clone())];
        if let Some(lsid) = &cmd.lsid {
            attrs.push(KeyValue::new(
                "db.mongodb.lsid",
                Bson::Document(lsid.clone())
                    .into_relaxed_extjson()
                    .to_string(),
            ));
        }
        if let Some(txn_number) = &cmd.txn_number {
            attrs.push(KeyValue::new("db.mongodb.txn_number", *txn_number));
        }
        Self {
            should_redact: cmd.should_redact(),
            name: cmd.name.clone(),
            attrs,
        }
    }
}
