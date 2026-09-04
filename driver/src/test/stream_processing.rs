//! Functional smoke test for Atlas Stream Processing.
//!
//! Skipped unless `MONGODB_STREAM_PROCESSING_URI` is set to a workspace
//! endpoint (`atlas-stream-*.<region>.a.query.mongodb{,-stage}.net`) with
//! valid credentials.
//!
//! Exercises the full lifecycle: create -> start -> stats -> sample -> stop -> drop.

use std::time::Duration;

use crate::{
    bson::{doc, oid::ObjectId, Document},
    stream_processing::StreamProcessingClient,
};

use super::log_uncaptured;

fn skip_msg() -> Option<String> {
    match std::env::var("MONGODB_STREAM_PROCESSING_URI") {
        Ok(uri) if !uri.is_empty() => {
            if StreamProcessingClient::is_workspace_uri(&uri) {
                None
            } else {
                Some(format!(
                    "MONGODB_STREAM_PROCESSING_URI=\"{uri}\" is not a workspace endpoint"
                ))
            }
        }
        _ => Some("MONGODB_STREAM_PROCESSING_URI is not configured".to_string()),
    }
}

async fn wait_for_state(
    processors: &crate::stream_processing::StreamProcessors,
    name: &str,
    target: &str,
    timeout: Duration,
) -> crate::error::Result<String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut state = processors.get_info(name).await?.state;
    while state != target && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        state = processors.get_info(name).await?.state;
    }
    Ok(state)
}

#[tokio::test]
async fn lifecycle() {
    if let Some(reason) = skip_msg() {
        log_uncaptured(format!("Skipping stream_processing::lifecycle: {reason}"));
        return;
    }

    let uri = std::env::var("MONGODB_STREAM_PROCESSING_URI").unwrap();
    let client = StreamProcessingClient::with_uri_str(&uri)
        .await
        .expect("StreamProcessingClient::with_uri_str");
    let processors = client.stream_processors();

    let name = format!("rustdriver_test_{}", ObjectId::new().to_hex());
    let pipeline = vec![
        doc! { "$source": { "connectionName": "sample_stream_solar" } },
        doc! { "$emit": { "connectionName": "__testLog", "topic": "rust-driver-demo" } },
    ];

    let cleanup = async {
        let _ = processors.get(&name).drop().await;
    };

    // create
    processors
        .create(&name, pipeline, None)
        .await
        .expect("create should succeed");

    let info = processors.get_info(&name).await.expect("get_info");
    assert_eq!(info.name, name);
    assert!(
        ["CREATED", "VALIDATING", "CREATING"]
            .iter()
            .any(|s| *s == info.state),
        "unexpected post-create state: {}",
        info.state
    );

    // start
    let processor = processors.get(&name);
    processor.start(None).await.expect("start should succeed");
    let state = wait_for_state(&processors, &name, "STARTED", Duration::from_secs(30))
        .await
        .expect("get_info while waiting");
    assert_eq!(state, "STARTED", "processor did not reach STARTED");

    // stats
    let stats: Document = processor.stats(None).await.expect("stats should succeed");
    assert!(!stats.is_empty());

    // sample: open + fetch one batch
    let samples = processor
        .samples(
            crate::stream_processing::GetStreamProcessorSamplesOptions::builder()
                .limit(5)
                .build(),
        )
        .await
        .expect("startSample");
    assert!(samples.cursor_id > 0, "expected non-zero cursor id");

    let batch = processor
        .samples(
            crate::stream_processing::GetStreamProcessorSamplesOptions::builder()
                .cursor_id(samples.cursor_id)
                .batch_size(5)
                .build(),
        )
        .await
        .expect("getMore");
    assert!(batch.cursor_id >= 0);

    // stop + drop
    processor.stop().await.expect("stop should succeed");
    processor.drop().await.expect("drop should succeed");

    // Cleanup is a best-effort no-op now that drop already ran; intentionally
    // run it regardless to make sure failures earlier in the test don't leak
    // a processor.
    cleanup.await;
}
