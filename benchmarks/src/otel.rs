use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};

#[cfg(feature = "otel-sdk-ratio")]
const SAMPLE_RATIO: f64 = 0.01;

pub(crate) struct OtelGuard {
    provider: SdkTracerProvider,
}

/// Must be run before tokio runtime initialization.
pub(crate) fn init() -> OtelGuard {
    unsafe {
        std::env::set_var("OTEL_RUST_INSTRUMENTATION_MONGODB_ENABLED", "true");
    }

    #[cfg(feature = "otel-sdk-ratio")]
    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(SAMPLE_RATIO)));
    #[cfg(feature = "otel-sdk-always")]
    let sampler = Sampler::AlwaysOn;

    let provider = SdkTracerProvider::builder().with_sampler(sampler).build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    OtelGuard { provider }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("OpenTelemetry shutdown failed: {e}");
        }
    }
}
