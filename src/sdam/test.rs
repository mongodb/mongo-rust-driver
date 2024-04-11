use std::{
    collections::HashSet,
    time::{Duration, Instant},
};

use bson::doc;
use semver::VersionReq;

use crate::{
    client::options::{ClientOptions, ServerAddress},
    cmap::RawCommandResponse,
    error::{Error, ErrorKind},
    event::{cmap::CmapEvent, sdam::SdamEvent},
    hello::{LEGACY_HELLO_COMMAND_NAME, LEGACY_HELLO_COMMAND_NAME_LOWERCASE},
    sdam::{ServerDescription, Topology},
    test::{
        get_client_options,
        log_uncaptured,
        util::event_buffer::EventBuffer,
        Event,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
    },
    Client,
};

#[tokio::test(flavor = "multi_thread")]
async fn min_heartbeat_frequency() {
    let mut setup_client_options = get_client_options().await.clone();
    if setup_client_options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping min_heartbeat_frequency test due to load-balanced topology");
        return;
    }
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !setup_client.supports_fail_command_appname_initial_handshake() {
        log_uncaptured(
            "skipping min_heartbeat_frequency test due to server not supporting failcommand \
             appname",
        );
        return;
    }

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMMinHeartbeatFrequencyTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::Times(5),
        fp_options,
    );

    let _fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let mut options = setup_client_options;
    options.app_name = Some("SDAMMinHeartbeatFrequencyTest".to_string());
    options.server_selection_timeout = Some(Duration::from_secs(5));
    let client = Client::with_options(options).expect("client creation succeeds");

    let start = Instant::now();
    client
        .database("admin")
        .run_command(doc! { "ping": 1 })
        .await
        .expect("ping should eventually succeed");

    let elapsed = Instant::now().duration_since(start).as_millis();
    assert!(
        elapsed >= 2000,
        "expected to take at least 2 seconds, instead took {}ms",
        elapsed
    );
    assert!(
        elapsed <= 3500,
        "expected to take at most 3.5 seconds, instead took {}ms",
        elapsed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sdam_pool_management() {
    let mut options = get_client_options().await.clone();
    if options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping sdam_pool_management test due to load-balanced topology");
        return;
    }
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.app_name = Some("SDAMPoolManagementTest".to_string());
    options.heartbeat_freq = Some(Duration::from_millis(50));

    let client = Client::test_builder()
        .additional_options(options, false)
        .await
        .min_heartbeat_freq(Duration::from_millis(50))
        .monitor_events()
        .build()
        .await;
    #[allow(deprecated)]
    let mut subscriber = client.events.subscribe_all();

    if !VersionReq::parse(">= 4.2.9")
        .unwrap()
        .matches(&client.server_version)
    {
        log_uncaptured(
            "skipping sdam_pool_management test due to server not supporting appName failCommand",
        );
        return;
    }

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolReady(_)))
        })
        .await
        .expect("should see pool ready event");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(_)))
        })
        .await
        .expect("should see server heartbeat succeeded event");

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMPoolManagementTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::Times(4),
        fp_options,
    );

    let _fp_guard = client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    // Since there is no deterministic ordering, simply collect all the events and check for their
    // presence.
    let events = subscriber
        .collect_events(Duration::from_secs(1), |_| true)
        .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::Sdam(SdamEvent::ServerHeartbeatFailed(_)))));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::Cmap(CmapEvent::PoolCleared(_)))));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::Cmap(CmapEvent::PoolReady(_)))));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(_)))));
}

#[tokio::test(flavor = "multi_thread")]
async fn hello_ok_true() {
    let mut setup_client_options = get_client_options().await.clone();
    setup_client_options.hosts.drain(1..);

    if setup_client_options.server_api.is_some() {
        log_uncaptured("skipping hello_ok_true test due to ServerApi being configured");
        return;
    }

    if setup_client_options.load_balanced == Some(true) {
        log_uncaptured("skipping hello_ok_true test due to load balanced topology");
        return;
    }

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;
    if !VersionReq::parse(">= 4.4.5")
        .unwrap()
        .matches(&setup_client.server_version)
    {
        log_uncaptured("skipping hello_ok_true test due to server not supporting hello");
        return;
    }

    let buffer = EventBuffer::new();
    #[allow(deprecated)]
    let mut subscriber = buffer.subscribe();

    let mut options = setup_client_options.clone();
    options.sdam_event_handler = Some(buffer.handler());
    options.direct_connection = Some(true);
    options.heartbeat_freq = Some(Duration::from_millis(500));
    let _client = Client::with_options(options).expect("client creation should succeed");

    // first heartbeat should be legacy hello but contain helloOk
    subscriber
        .wait_for_event(Duration::from_millis(2000), |event| {
            if let Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(e)) = event {
                assert_eq!(e.reply.get_bool("helloOk"), Ok(true));
                assert!(e.reply.get(LEGACY_HELLO_COMMAND_NAME_LOWERCASE).is_some());
                assert!(e.reply.get("isWritablePrimary").is_none());
                return true;
            }
            false
        })
        .await
        .expect("first heartbeat reply should contain helloOk: true");

    // subsequent heartbeats should just be hello
    for _ in 0..3 {
        subscriber
            .wait_for_event(Duration::from_millis(2000), |event| {
                if let Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(e)) = event {
                    assert!(e.reply.get("isWritablePrimary").is_some());
                    assert!(e.reply.get(LEGACY_HELLO_COMMAND_NAME_LOWERCASE).is_none());
                    return true;
                }
                false
            })
            .await
            .expect("subsequent heartbeats should use hello");
    }
}

#[tokio::test]
async fn repl_set_name_mismatch() -> crate::error::Result<()> {
    let client = TestClient::new().await;
    if !client.is_replica_set() {
        log_uncaptured("skipping repl_set_name_mismatch due to non-replica set topology");
        return Ok(());
    }

    let mut options = get_client_options().await.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.repl_set_name = Some("invalid".to_string());
    options.server_selection_timeout = Some(Duration::from_secs(5));
    let client = Client::with_options(options)?;
    let result = client.list_database_names().await;
    assert!(
        match result {
            Err(Error { ref kind, .. }) => matches!(**kind, ErrorKind::ServerSelection { .. }),
            _ => false,
        },
        "Unexpected result {:?}",
        result
    );

    Ok(())
}

/// Test verifying that a server's monitor stops after the server has been removed from the
/// topology.
#[tokio::test(flavor = "multi_thread")]
async fn removed_server_monitor_stops() -> crate::error::Result<()> {
    let buffer = EventBuffer::new();
    let options = ClientOptions::builder()
        .hosts(vec![
            ServerAddress::parse("localhost:49152")?,
            ServerAddress::parse("localhost:49153")?,
            ServerAddress::parse("localhost:49154")?,
        ])
        .heartbeat_freq(Duration::from_millis(50))
        .sdam_event_handler(buffer.handler())
        .repl_set_name("foo".to_string())
        .build();

    let hosts = options.hosts.clone();
    let set_name = options.repl_set_name.clone().unwrap();

    #[allow(deprecated)]
    let mut subscriber = buffer.subscribe();
    let topology = Topology::new(options)?;

    // Wait until all three monitors have started.
    let mut seen_monitors = HashSet::new();
    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            if let Event::Sdam(SdamEvent::ServerHeartbeatStarted(e)) = event {
                seen_monitors.insert(e.server_address.clone());
            }
            seen_monitors.len() == hosts.len()
        })
        .await
        .expect("should see all three monitors start");

    // Remove the third host from the topology.
    let hello = doc! {
        "ok": 1,
        "isWritablePrimary": true,
        "hosts": [
            hosts[0].clone().to_string(),
            hosts[1].clone().to_string(),
        ],
        "me": hosts[0].clone().to_string(),
        "setName": set_name,
        "maxBsonObjectSize": 1234,
        "maxWriteBatchSize": 1234,
        "maxMessageSizeBytes": 1234,
        "minWireVersion": 0,
        "maxWireVersion": 13,
    };
    let hello_reply = RawCommandResponse::with_document_and_address(hosts[0].clone(), hello)
        .unwrap()
        .into_hello_reply()
        .unwrap();

    topology
        .clone_updater()
        .update(ServerDescription::new_from_hello_reply(
            hosts[0].clone(),
            hello_reply,
            Duration::from_millis(10),
        ))
        .await;

    subscriber.wait_for_event(Duration::from_secs(1), |event| {
        matches!(event, Event::Sdam(SdamEvent::ServerClosed(e)) if e.address == hosts[2])
    }).await.expect("should see server closed event");

    // Capture heartbeat events for 1 second. The monitor for the removed server should stop
    // publishing them.
    let events = subscriber.collect_events(Duration::from_secs(1), |event| {
        matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatStarted(e)) if e.server_address == hosts[2])
    }).await;

    // Use 3 to account for any heartbeats that happen to start between emitting the ServerClosed
    // event and actually publishing the state with the closed server.
    assert!(
        events.len() < 3,
        "expected monitor for removed server to stop performing checks, but saw {} heartbeats",
        events.len()
    );

    Ok(())
}
