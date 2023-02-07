use std::{sync::Arc, time::Duration};

use bson::doc;
use tokio::sync::RwLockWriteGuard;

use crate::{
    error::Result,
    event::{
        cmap::{CmapEvent, CmapEventHandler, ConnectionCheckoutFailedReason},
        command::CommandEventHandler,
    },
    runtime,
    runtime::AsyncJoinHandle,
    test::{
        log_uncaptured,
        Event,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
};

use super::{run_spec_test_with_path, run_unified_format_test, run_v2_test};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_legacy() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["retryable-reads", "legacy"], run_v2_test).await;
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(&["retryable-reads", "unified"], run_unified_format_test).await;
}

/// Test ensures that the connection used in the first attempt of a retry is released back into the
/// pool before the second attempt.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_releases_connection() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let mut client_options = CLIENT_OPTIONS.get().await.clone();
    client_options.hosts.drain(1..);
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);

    let client = TestClient::with_options(Some(client_options)).await;
    if !client.supports_fail_command() {
        log_uncaptured("skipping retry_releases_connection due to failCommand not being supported");
        return;
    }

    let collection = client
        .database("retry_releases_connection")
        .collection("retry_releases_connection");
    collection.insert_one(doc! { "x": 1 }, None).await.unwrap();

    // Use a connection error to ensure streaming monitor checks get cancelled. Otherwise, we'd have
    // to wait for the entire heartbeatFrequencyMS before the find succeeds.
    let options = FailCommandOptions::builder().close_connection(true).build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    runtime::timeout(Duration::from_secs(1), collection.find_one(doc! {}, None))
        .await
        .expect("operation should not time out")
        .expect("find should succeed");
}

/// Prose test from retryable reads spec verifying that PoolClearedErrors are retried.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn retry_read_pool_cleared() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;

    let handler = Arc::new(EventHandler::new());

    let mut client_options = CLIENT_OPTIONS.get().await.clone();
    client_options.retry_reads = Some(true);
    client_options.max_pool_size = Some(1);
    client_options.cmap_event_handler = Some(handler.clone() as Arc<dyn CmapEventHandler>);
    client_options.command_event_handler = Some(handler.clone() as Arc<dyn CommandEventHandler>);
    // on sharded clusters, ensure only a single mongos is used
    if client_options.repl_set_name.is_none() {
        client_options.hosts.drain(1..);
    }

    let client = TestClient::with_options(Some(client_options.clone())).await;
    if !client.supports_block_connection() {
        log_uncaptured(
            "skipping retry_read_pool_cleared due to blockConnection not being supported",
        );
        return;
    }
    if client.is_load_balanced() {
        log_uncaptured("skipping retry_read_pool_cleared due to load-balanced topology");
        return;
    }

    let collection = client
        .database("retry_read_pool_cleared")
        .collection("retry_read_pool_cleared");
    collection.insert_one(doc! { "x": 1 }, None).await.unwrap();

    let options = FailCommandOptions::builder()
        .error_code(91)
        .block_connection(Duration::from_secs(1))
        .build();
    let failpoint = FailPoint::fail_command(&["find"], FailPointMode::Times(1), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let mut subscriber = handler.subscribe();

    let mut tasks: Vec<AsyncJoinHandle<_>> = Vec::new();
    for _ in 0..2 {
        let coll = collection.clone();
        let task = runtime::spawn(async move { coll.find_one(doc! {}, None).await });
        tasks.push(task);
    }

    futures::future::join_all(tasks)
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()
        .expect("all should succeed");

    let _ = subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::ConnectionCheckedOut(_)))
        })
        .await
        .expect("first checkout should succeed");

    let _ = subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
        })
        .await
        .expect("pool clear should occur");

    let _ = subscriber
        .wait_for_event(Duration::from_millis(1000), |event| match event {
            Event::Cmap(CmapEvent::ConnectionCheckoutFailed(e)) => {
                matches!(e.reason, ConnectionCheckoutFailedReason::ConnectionError)
            }
            _ => false,
        })
        .await
        .expect("second checkout should fail");

    assert_eq!(handler.get_command_started_events(&["find"]).len(), 3);
}
