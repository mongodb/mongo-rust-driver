use std::sync::LazyLock;

pub(crate) static TRACER: LazyLock<opentelemetry::global::BoxedTracer> = LazyLock::new(|| {
    opentelemetry::global::tracer_with_scope(
        opentelemetry::InstrumentationScope::builder("mongodb")
            .with_version(env!("CARGO_PKG_VERSION"))
            .build(),
    )
});

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
