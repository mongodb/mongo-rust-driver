use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use opentelemetry_sdk::{
    error::OTelSdkResult,
    trace::{Sampler, SdkTracerProvider, SpanData, SpanExporter},
};

#[cfg(feature = "otel-sdk-ratio")]
const SAMPLE_RATIO: f64 = 0.01;

pub(crate) struct OtelGuard {
    provider: SdkTracerProvider,
    exported: Arc<AtomicU64>,
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

    let exporter = CountingExporter::new();
    let exported = exporter.exported.clone();
    let provider = SdkTracerProvider::builder()
        .with_sampler(sampler)
        .with_batch_exporter(exporter)
        .build();
    opentelemetry::global::set_tracer_provider(provider.clone());
    OtelGuard { provider, exported }
}

impl Drop for OtelGuard {
    fn drop(&mut self) {
        if let Err(e) = self.provider.shutdown() {
            eprintln!("OpenTelemetry shutdown failed: {e}");
        }
        println!(
            "OpenTelemetry: spans exported={}",
            self.exported.load(Ordering::Relaxed)
        );
    }
}

#[derive(Debug)]
struct CountingExporter {
    exported: Arc<AtomicU64>,
}

impl CountingExporter {
    fn new() -> Self {
        Self {
            exported: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl SpanExporter for CountingExporter {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        self.exported
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}
