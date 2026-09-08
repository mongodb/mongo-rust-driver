//! Demonstrates the full lifecycle of an Atlas Stream Processing (ASP) stream
//! processor using [`mongodb::stream_processing::StreamProcessingClient`].
//! It creates, starts, samples, stops, and drops a processor.
//!
//! Requirements:
//!
//!   - An Atlas Stream Processing workspace with a hostname matching the pattern
//!     `atlas-stream-<workspaceId>-<suffix>.<region>.a.query.mongodb.net` (or `.mongodb-<env>.net`
//!     for staging).
//!   - A user with the `atlasAdmin` role.
//!   - Two connections registered in the workspace:
//!       - `sample_stream_solar`  (built-in sample source)
//!       - `__testLog`            (built-in test sink)
//!
//! Use the `MONGODB_STREAM_PROCESSING_URI` environment variable to specify
//! the workspace connection string (including username/password). Run with:
//!
//! ```text
//! MONGODB_STREAM_PROCESSING_URI='mongodb://user:pass@atlas-stream-….a.query.mongodb.net/' \
//!     cargo run -p mongodb --example stream_processing_example
//! ```

use std::{
    process::ExitCode,
    time::{Duration, Instant},
};

use mongodb::{
    bson::{doc, oid::ObjectId},
    error::Result,
    stream_processing::{
        GetStreamProcessorSamplesOptions,
        StreamProcessingClient,
        StreamProcessors,
    },
};

#[tokio::main]
async fn main() -> ExitCode {
    let uri = match std::env::var("MONGODB_STREAM_PROCESSING_URI") {
        Ok(uri) if !uri.is_empty() => uri,
        _ => {
            eprintln!("This example requires an Atlas Stream Processing workspace endpoint.");
            eprintln!("Set MONGODB_STREAM_PROCESSING_URI to the workspace connection string.");
            return ExitCode::from(1);
        }
    };

    if !StreamProcessingClient::is_workspace_uri(&uri) {
        eprintln!("MONGODB_STREAM_PROCESSING_URI does not look like a workspace endpoint.");
        eprintln!(
            "Expected hostname pattern: atlas-stream-*.<region>.a.query.mongodb.net (or \
             .mongodb-stage.net for Atlas staging)"
        );
        return ExitCode::from(1);
    }

    let client = match StreamProcessingClient::with_uri_str(&uri).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("StreamProcessingClient::with_uri_str failed: {e}");
            return ExitCode::from(1);
        }
    };

    let processors = client.stream_processors();
    let name = format!("rustdriver_demo_{}", ObjectId::new().to_hex());

    println!("Workspace: {uri}");
    println!("Processor: {name}\n");

    match run_lifecycle(&processors, &name).await {
        Ok(()) => {
            println!("OK.");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("\nFAILED: {e}");
            // Best-effort cleanup so we don't leave processors behind.
            if processors.get(&name).drop().await.is_ok() {
                eprintln!("(cleaned up processor {name})");
            }
            ExitCode::from(1)
        }
    }
}

async fn run_lifecycle(processors: &StreamProcessors, name: &str) -> Result<()> {
    let pipeline = vec![
        doc! { "$source": { "connectionName": "sample_stream_solar" } },
        doc! { "$emit": { "connectionName": "__testLog", "topic": "rust-driver-demo" } },
    ];

    // 1. create
    println!("[1/6] create({name})");
    processors.create(name, pipeline, None).await?;
    let info = processors.get_info(name).await?;
    println!("      state={}\n", info.state);

    // 2. start
    println!("[2/6] start()");
    let processor = processors.get(name);
    processor.start(None).await?;
    let state = wait_for_state(processors, name, "STARTED", Duration::from_secs(30)).await?;
    println!("      state={state}\n");
    if state != "STARTED" {
        return Err(mongodb::error::Error::custom(format!(
            "processor did not reach STARTED within 30s (got {state})"
        )));
    }

    // 3. stats
    println!("[3/6] stats()");
    let stats = processor.stats(None).await?;
    println!("      {stats:?}\n");

    // 4. sample
    println!("[4/6] samples()");
    let opened = processor
        .samples(GetStreamProcessorSamplesOptions::builder().limit(5).build())
        .await?;
    println!(
        "      open  cursorId={} docs={}",
        opened.cursor_id,
        opened.documents.len()
    );

    if !opened.is_exhausted() {
        // Give the stream a moment to produce something.
        tokio::time::sleep(Duration::from_secs(2)).await;
        let batch = processor
            .samples(
                GetStreamProcessorSamplesOptions::builder()
                    .cursor_id(opened.cursor_id)
                    .batch_size(5)
                    .build(),
            )
            .await?;
        println!(
            "      batch cursorId={} docs={}",
            batch.cursor_id,
            batch.documents.len()
        );
        for (i, doc) in batch.documents.iter().enumerate() {
            println!("          [{i}] {doc:?}");
        }
    }
    println!();

    // 5. stop
    println!("[5/6] stop()");
    processor.stop().await?;
    let state = processors.get_info(name).await?.state;
    println!("      state={state}\n");

    // 6. drop
    println!("[6/6] drop()");
    processor.drop().await?;
    println!("      dropped\n");

    Ok(())
}

async fn wait_for_state(
    processors: &StreamProcessors,
    name: &str,
    target: &str,
    timeout: Duration,
) -> Result<String> {
    let deadline = Instant::now() + timeout;
    let mut state = processors.get_info(name).await?.state;
    while state != target && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(500)).await;
        state = processors.get_info(name).await?.state;
    }
    Ok(state)
}
