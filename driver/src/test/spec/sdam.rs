use std::time::Duration;

use crate::{
    bson::{doc, Document},
    error::{Error, Result},
    event::{cmap::CmapEvent, sdam::SdamEvent},
    hello::LEGACY_HELLO_COMMAND_NAME,
    runtime,
    test::{
        get_client_options,
        log_uncaptured,
        server_version_lt,
        spec::unified_runner::run_unified_tests,
        streaming_monitor_protocol_supported,
        topology_is_load_balanced,
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
        EventClient,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
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
            "apply backpressure on network timeout error during connection establishment",
            // TODO RUST-2068: unskip these tests
            "Pool is cleared on handshake error during minPoolSize population",
            "Pool is cleared on authentication error during minPoolSize population",
        ])
        .await;
}

/// Streaming protocol prose test 1 from SDAM spec tests.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_min_heartbeat_frequency() {
    if topology_is_load_balanced().await {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let buffer = EventBuffer::new();
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.sdam_event_handler = Some(buffer.handler());

    let hosts = options.hosts.clone();

    let client = Client::with_options(options).unwrap();
    // discover a server
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();

    // For each server in the topology, start a task that ensures heartbeats happen roughly every
    // 500ms for 5 heartbeats.
    let mut tasks = Vec::new();
    for address in hosts {
        let h = buffer.clone();
        tasks.push(runtime::spawn(async move {

            let mut event_stream = h.stream();
            for _ in 0..5 {
                let event = event_stream
                    .next_match(Duration::from_millis(750), |e| {
                        matches!(e, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(e)) if e.server_address == address)
                    })
                    .await;
                if event.is_none() {
                    return Err(format!("timed out waiting for heartbeat from {address}"));
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
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_frequency_is_respected() {
    if topology_is_load_balanced().await {
        log_uncaptured("skipping streaming_min_heartbeat_frequency due to load balanced topology");
        return;
    }

    let buffer = EventBuffer::new();
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(1000));
    options.sdam_event_handler = Some(buffer.handler());

    let hosts = options.hosts.clone();

    let client = Client::with_options(options).unwrap();
    // discover a server
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .unwrap();

    // For each server in the topology, start a task that ensures heartbeats happen roughly every
    // 1s for 3s.
    let mut tasks = Vec::new();
    for address in hosts {
        let h = buffer.clone();
        tasks.push(runtime::spawn(async move {

            let mut event_stream = h.stream();

            // collect events for 2 seconds, should see between 2 and 3 heartbeats.
            let events = event_stream.collect(Duration::from_secs(3), |e| {
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
#[tokio::test(flavor = "multi_thread")]
async fn rtt_is_updated() {
    if !streaming_monitor_protocol_supported().await {
        log_uncaptured(
            "skipping rtt_is_updated due to not supporting streaming monitoring protocol",
        );
        return;
    }
    if topology_is_load_balanced().await {
        log_uncaptured("skipping rtt_is_updated due to load balanced topology");
        return;
    }

    let app_name = "streamingRttTest";

    let buffer = EventBuffer::new();
    let mut options = get_client_options().await.clone();
    options.heartbeat_freq = Some(Duration::from_millis(500));
    options.app_name = Some(app_name.to_string());
    options.sdam_event_handler = Some(buffer.handler());
    options.hosts.drain(1..);
    options.direct_connection = Some(true);

    let host = options.hosts[0].clone();

    let client = Client::with_options(options).unwrap();

    let mut event_stream = buffer.stream();

    // run a find to wait for the primary to be discovered
    client
        .database("foo")
        .collection::<Document>("bar")
        .find(doc! {})
        .await
        .unwrap();

    // wait for multiple heartbeats, assert their RTT is > 0
    let events = event_stream
        .collect(Duration::from_secs(2), |e| {
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
    let fail_point = FailPoint::fail_command(
        &["hello", LEGACY_HELLO_COMMAND_NAME],
        FailPointMode::Times(1000),
    )
    .block_connection(Duration::from_millis(500))
    .app_name(app_name);
    let _guard = client.enable_fail_point(fail_point).await.unwrap();

    let mut watcher = client.topology().watcher().clone();
    watcher.observe_latest(); // start the monitor from most recent state
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

/* TODO RUST-1895 enable this
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_started_before_socket() {
    use std::sync::{Arc, Mutex};
    use tokio::{io::AsyncReadExt, net::TcpListener};

    #[derive(Debug, PartialEq)]
    enum Event {
        ClientConnected,
        ClientHelloReceived,
        HeartbeatStarted,
        HeartbeatFailed,
    }
    let events: Arc<Mutex<Vec<Event>>> = Arc::new(Mutex::new(vec![]));

    // Mock server
    {
        let listener = TcpListener::bind("127.0.0.1:9999").await.unwrap();
        let events = Arc::clone(&events);
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                events.lock().unwrap().push(Event::ClientConnected);
                let mut buf = [0; 1024];
                let _ = socket.read(&mut buf).await.unwrap();
                events.lock().unwrap().push(Event::ClientHelloReceived);
            }
        });
    }

    // Client setup
    let mut options = ClientOptions::parse("mongodb://127.0.0.1:9999/")
        .await
        .unwrap();
    options.server_selection_timeout = Some(Duration::from_millis(500));
    {
        let events = Arc::clone(&events);
        options.sdam_event_handler =
            Some(crate::event::EventHandler::callback(move |ev| match ev {
                SdamEvent::ServerHeartbeatStarted(_) => {
                    events.lock().unwrap().push(Event::HeartbeatStarted)
                }
                SdamEvent::ServerHeartbeatFailed(_) => {
                    events.lock().unwrap().push(Event::HeartbeatFailed)
                }
                _ => (),
            }));
    }
    let client = Client::with_options(options).unwrap();

    // Test event order
    let _ = client.list_database_names().await;
    assert_eq!(
        &[
            Event::HeartbeatStarted,
            Event::ClientConnected,
            Event::ClientHelloReceived,
            Event::HeartbeatFailed
        ],
        &events.lock().unwrap()[0..4],
    );
}
*/

#[tokio::test(flavor = "multi_thread")]
async fn connection_pool_backpressure() {
    if server_version_lt(7, 0).await {
        log_uncaptured("skipping connection_pool_backpressure: requires 7.0+");
        return;
    }

    let mut options = get_client_options().await.clone();
    options.max_connecting = Some(100);
    // the driver has a default max pool size of 10 rather than the spec's default of 100
    options.max_pool_size = Some(100);
    let client = Client::for_test().options(options).monitor_events().await;

    async fn run_test(client: &EventClient) -> Result<()> {
        let admin = client.database("admin");
        admin
            .run_command(
                doc! { "setParameter": 1, "ingressConnectionEstablishmentRateLimiterEnabled":
                true },
            )
            .await?;
        admin
            .run_command(
                doc! { "setParameter": 1, "ingressConnectionEstablishmentRatePerSec":
                20},
            )
            .await?;
        admin
            .run_command(
                doc! { "setParameter": 1, "ingressConnectionEstablishmentBurstCapacitySecs": 1 },
            )
            .await?;
        admin
            .run_command(
                doc! { "setParameter": 1, "ingressConnectionEstablishmentMaxQueueDepth": 1 },
            )
            .await?;

        let coll = client.database("test").collection("test");
        coll.insert_one(doc! {}).await?;

        let mut tasks = Vec::new();
        for _ in 0..100 {
            let coll = coll.clone();
            tasks.push(tokio::task::spawn(async move {
                let _ = coll
                    .find_one(doc! { "$where": "sleep(2000) || true" })
                    .await;
            }));
        }
        futures::future::join_all(tasks).await;

        let events = &client.events;

        let checkout_failed_events = events.filter_map(|e| match e {
            Event::Cmap(CmapEvent::ConnectionCheckoutFailed(e)) => Some(e.clone()),
            _ => None,
        });
        if checkout_failed_events.len() < 10 {
            return Err(Error::internal(format!(
                "expected >= 10 checkout failed events, got {}",
                checkout_failed_events.len()
            )));
        }

        let pool_cleared_events = events.filter_map(|e| match e {
            Event::Cmap(CmapEvent::PoolCleared(e)) => Some(e.clone()),
            _ => None,
        });
        if !pool_cleared_events.is_empty() {
            dbg!(&pool_cleared_events);
            return Err(Error::internal(format!(
                "expected no pool cleared events, got {}",
                pool_cleared_events.len()
            )));
        }

        Ok(())
    }
    let result = run_test(&client).await;

    tokio::time::sleep(Duration::from_secs(1)).await;
    client
        .database("admin")
        .run_command(
            doc! { "setParameter": 1, "ingressConnectionEstablishmentRateLimiterEnabled": false
            },
        )
        .await
        .unwrap();

    result.unwrap();
}
