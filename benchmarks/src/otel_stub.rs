pub(crate) struct OtelGuard {}

/// Must be run before tokio runtime initialization.
pub(crate) fn init() -> OtelGuard {
    unsafe {
        std::env::set_var("OTEL_RUST_INSTRUMENTATION_MONGODB_ENABLED", "true");
    }
    OtelGuard {}
}
