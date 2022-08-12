use std::{sync::Arc, time::Duration};

use bson::{doc, Document};
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{run_spec_test_with_path, run_unified_format_test_filtered};
use crate::{
    hello::LEGACY_HELLO_COMMAND_NAME,
    runtime,
    test::{
        log_uncaptured,
        Event,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        SdamEvent,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    Client,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    let _guard: RwLockWriteGuard<()> = LOCK.run_exclusively().await;
    run_spec_test_with_path(
        &["server-discovery-and-monitoring", "unified"],
        |path, t| {
            run_unified_format_test_filtered(path, t, |test| {
                // skipped because we don't support socketTimeoutMS
                test.description.as_str() != "Ignore network timeout error on find"
            })
        },
    )
    .await;
}

/// Streaming protocol prose test 1 from SDAM spec tests.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn streaming_min_heartbeat_frequency() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let test_client = TestClient::new().await;
    if test_client.is_load_balanced() {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let handler = Arc::new(EventHandler::new());
    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.sdam_event_handler = Some(handler.clone());

    let hosts = options.hosts.clone();

    let client = Client::with_options(options).unwrap();
    // discover a server
    client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .unwrap();

    // For each server in the topology, start a task that ensures heartbeats happen roughly every
    // 500ms for 5 heartbeats.
    let mut tasks = Vec::new();
    for address in hosts {
        let h = handler.clone();
        tasks.push(runtime::spawn(async move {
            let mut subscriber = h.subscribe();
            for _ in 0..5 {
                let event = subscriber
                    .wait_for_event(Duration::from_millis(750), |e| {
                        matches!(e, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(e)) if e.server_address == address)
                    })
                    .await;
                if event.is_none() {
                    return Err(format!("timed out waiting for heartbeat from {}", address));
                }
            }
            Ok(())
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// Variant of the previous prose test that checks for a non-minHeartbeatFrequencyMS value.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn heartbeat_frequency_is_respected() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let test_client = TestClient::new().await;
    if test_client.is_load_balanced() {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let handler = Arc::new(EventHandler::new());
    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(1000));
    options.sdam_event_handler = Some(handler.clone());

    let hosts = options.hosts.clone();

    let client = Client::with_options(options).unwrap();
    // discover a server
    client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .unwrap();

    // For each server in the topology, start a task that ensures heartbeats happen roughly every
    // 1s for 3s.
    let mut tasks = Vec::new();
    for address in hosts {
        let h = handler.clone();
        tasks.push(runtime::spawn(async move {
            let mut subscriber = h.subscribe();

            // collect events for 2 seconds, should see between 2 and 3 heartbeats.
            let events = subscriber.collect_events(Duration::from_secs(3), |e| {
                matches!(e, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(e)) if e.server_address == address)
            }).await;

            if !(2..=3).contains(&events.len()) {
                return Err(format!("expected 1 or 2 heartbeats, but got {}", events.len()));
            }

            Ok(())
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// RTT prose test 1 from SDAM spec tests.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn rtt_is_updated() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let test_client = TestClient::new().await;
    if test_client.server_version_lt(4, 4) {
        log_uncaptured("skipping rtt_is_updated due to server version less than 4.4");
        return;
    }

    if test_client.is_load_balanced() {
        log_uncaptured("skipping rtt_is_updated due to load balanced topology");
        return;
    }

    let app_name = "streamingRttTest";

    let handler = Arc::new(EventHandler::new());
    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.app_name = Some(app_name.to_string());
    options.sdam_event_handler = Some(handler.clone());
    options.hosts.drain(1..);
    options.direct_connection = Some(true);

    let host = options.hosts[0].clone();

    let client = Client::with_options(options).unwrap();
    let mut subscriber = handler.subscribe();

    // run a find to wait for the primary to be discovered
    client
        .database("foo")
        .collection::<Document>("bar")
        .find(None, None)
        .await
        .unwrap();

    // wait for multiple heartbeats, assert their RTT is > 0
    let events = subscriber
        .collect_events(Duration::from_secs(2), |e| {
            if let Event::Sdam(SdamEvent::ServerDescriptionChanged(e)) = e {
                assert!(
                    e.new_description.average_round_trip_time().unwrap() > Duration::from_millis(0)
                );
            };
            true
        })
        .await;
    assert!(!events.is_empty());

    // configure a failpoint that blocks hello commands
    let fp = FailPoint::fail_command(
        &["hello", LEGACY_HELLO_COMMAND_NAME],
        FailPointMode::Times(1000),
        FailCommandOptions::builder()
            .block_connection(Duration::from_millis(500))
            .app_name(app_name.to_string())
            .build(),
    );
    let _gp_guard = fp.enable(&client, None).await.unwrap();

    let mut watcher = client.topology().watch();
    runtime::timeout(Duration::from_secs(10), async move {
        loop {
            watcher.wait_for_update(Duration::MAX).await;
            let rtt = watcher
                .server_description(&host)
                .unwrap()
                .average_round_trip_time
                .unwrap();

            if rtt > Duration::from_millis(250) {
                break;
            }
        }
    })
    .await
    .unwrap();
}
