use std::time::Duration;

use bson::{doc, Document};

use crate::{
    event::sdam::SdamEvent,
    hello::LEGACY_HELLO_COMMAND_NAME,
    options::ClientOptions,
    runtime,
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::run_unified_tests,
        util::{
            event_buffer::EventBuffer,
            fail_point::{FailPoint, FailPointMode},
        },
        Event,
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
            // Flaky tests
            "Reset server and pool after network timeout error during authentication",
            //"Ignore network timeout error on find",
        ])
        .await;
}

/// Streaming protocol prose test 1 from SDAM spec tests.
#[tokio::test(flavor = "multi_thread")]
async fn streaming_min_heartbeat_frequency() {
    let test_client = Client::for_test().await;
    if test_client.is_load_balanced() {
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
#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_frequency_is_respected() {
    let test_client = Client::for_test().await;
    if test_client.is_load_balanced() {
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
    let test_client = Client::for_test().await;
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

#[tokio::test(flavor = "multi_thread")]
async fn socket_timeout_ms_uri_option() {
    let uri = "mongodb+srv://test1.test.build.10gen.cc/?socketTimeoutMs=1";
    let options = ClientOptions::parse(uri).await.unwrap();
    assert_eq!(options.socket_timeout.unwrap().as_millis(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn socket_timeout_ms_client_option() {
    let mut options = get_client_options().await.clone();
    options.socket_timeout = Some(Duration::from_millis(1));

    let client = Client::with_options(options.clone()).unwrap();
    let db = client.database("test");
    let error = db
        .run_command(doc! {"ping": 1})
        .await
        .expect_err("should fail with socket timeout error");
    let error_description = format!("{}", error);
    for host in options.hosts.iter() {
        assert!(error_description.contains(format!("{}", host).as_str()));
    }
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
