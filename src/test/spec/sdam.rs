use std::{sync::Arc, time::Duration};

use bson::{doc, Document};

use crate::{
    hello::LEGACY_HELLO_COMMAND_NAME,
    runtime,
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        Event,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
    },
    Client, event::sdam::SdamEvent,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn run_unified() {
    // TODO RUST-1222: Unskip this file
    let mut skipped_files = vec!["interruptInUse-pool-clear.json"];
    if cfg!(not(feature = "tracing-unstable")) {
        skipped_files.extend_from_slice(&[
            "logging-standalone.json",
            "logging-replicaset.json",
            "logging-sharded.json",
            "logging-loadbalanced.json",
        ]);
    }

    run_unified_tests(&["server-discovery-and-monitoring", "unified"])
        .skip_files(&skipped_files)
        .skip_tests(&[
            // The driver does not support socketTimeoutMS.
            "Reset server and pool after network timeout error during authentication",
            "Ignore network timeout error on find",
        ])
        .await;
}

/// Streaming protocol prose test 1 from SDAM spec tests.
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn streaming_min_heartbeat_frequency() {
    let test_client = TestClient::new().await;
    if test_client.is_load_balanced() {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let handler = Arc::new(EventHandler::new());
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.sdam_event_handler = Some(handler.clone().into());

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
    let test_client = TestClient::new().await;
    if test_client.is_load_balanced() {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let handler = Arc::new(EventHandler::new());
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(1000));
    options.sdam_event_handler = Some(handler.clone().into());

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
    let test_client = TestClient::new().await;
    if !test_client.supports_streaming_monitoring_protocol() {
        log_uncaptured(
            "skipping rtt_is_updated due to not supporting streaming monitoring protocol",
        );
        return;
    }

    if test_client.is_load_balanced() {
        log_uncaptured("skipping rtt_is_updated due to load balanced topology");
        return;
    }

    if test_client.supports_block_connection() {
        log_uncaptured("skipping rtt_is_updated due to not supporting block_connection");
        return;
    }

    let app_name = "streamingRttTest";

    let handler = Arc::new(EventHandler::new());
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.app_name = Some(app_name.to_string());
    options.sdam_event_handler = Some(handler.clone().into());
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
    assert!(events.len() > 2);

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
            watcher.wait_for_update(None).await;
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
