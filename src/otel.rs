//! Support for OpenTelemetry.

use std::sync::LazyLock;

use opentelemetry::{
    global::BoxedTracer,
    trace::{Span as _, Tracer as _},
    KeyValue,
};

use crate::{error::Result, operation::Operation, options::ClientOptions, Client};

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

static TRACER: LazyLock<BoxedTracer> = LazyLock::new(|| {
    opentelemetry::global::tracer_with_scope(
        opentelemetry::InstrumentationScope::builder("mongodb")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    )
});

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

    pub(crate) fn otel_query_text_max_length(&self) -> usize {
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

impl Client {
    pub(crate) fn start_operation_span(&self, op: &impl Operation) -> Span {
        if !self.options().otel_enabled() {
            return Span { inner: None };
        }
        let span_name = if let Some(coll) = op.collection() {
            format!("{} {}.{}", op.name(), op.database(), coll)
        } else {
            format!("{} {}", op.name(), op.database())
        };
        let mut attrs = vec![
            KeyValue::new("db.system", "mongodb"),
            KeyValue::new("db.namespace", op.database().to_owned()),
            KeyValue::new("db.operation.name", op.name().as_str().to_owned()),
            KeyValue::new("db.operation.summary", span_name.clone()),
        ];
        if let Some(coll) = op.collection() {
            attrs.push(KeyValue::new("db.collection.name", coll.to_owned()));
        }
        Span {
            inner: Some(
                TRACER
                    .span_builder(span_name)
                    .with_kind(opentelemetry::trace::SpanKind::Client)
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
    pub(crate) async fn record_error<Out>(
        &mut self,
        code: impl AsyncFnOnce() -> Result<Out>,
    ) -> Result<Out> {
        let result = code().await;
        if let (Some(inner), Err(error)) = (&mut self.inner, &result) {
            inner.set_attributes([
                KeyValue::new("exception.message", error.to_string()),
                KeyValue::new("exception.type", error.kind.name()),
                #[cfg(test)]
                KeyValue::new("exception.backtrace", error.bt.to_string()),
            ]);
        }
        result
    }
}
