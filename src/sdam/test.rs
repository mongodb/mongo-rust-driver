use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bson::{bson, doc};
use semver::VersionReq;
use tokio::sync::RwLockWriteGuard;

use crate::{
    error::ErrorKind,
    test::{
        CmapEvent,
        Event,
        EventClient,
        EventHandler,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
        CLIENT_OPTIONS,
        LOCK,
    },
    Client,
    RUNTIME,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn min_heartbeat_frequency() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !setup_client.supports_fail_command().await {
        println!("skipping min_heartbeat_frequency test due to server not supporting fail points");
        return;
    }

    if setup_client.server_version_lt(4, 9) {
        println!("skipping min_heartbeat_frequency test due to server version being less than 4.9");
        return;
    }

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMMinHeartbeatFrequencyTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Times(5), fp_options);

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
        .run_command(doc! { "ping": 1 }, None)
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

// TODO: RUST-232 update this test to incorporate SDAM events
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_pool_management() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.app_name = Some("SDAMPoolManagementTest".to_string());
    options.heartbeat_freq = Some(Duration::from_millis(50));

    let event_handler = EventHandler::new();
    let mut subscriber = event_handler.subscribe();

    let client = EventClient::with_additional_options(
        Some(options),
        Some(Duration::from_millis(50)),
        None,
        event_handler.clone(),
    )
    .await;

    if !VersionReq::parse(">= 4.2.9")
        .unwrap()
        .matches(&client.server_version)
    {
        println!(
            "skipping sdam_pool_management test due to server not supporting appName failCommand"
        );
        return;
    }

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMPoolManagementTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Times(1), fp_options);

    let _fp_guard = client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");
}

// prose version of minPoolSize-error.yml SDAM integration test
// TODO: RUST-232 replace this test with the spec runner
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_min_pool_size_error() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !VersionReq::parse(">= 4.9.0")
        .unwrap()
        .matches(&setup_client.server_version)
    {
        println!(
            "skipping sdam_pool_management test due to server not supporting appName failCommand"
        );
        return;
    }

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMMinPoolSizeErrorTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Skip(3), fp_options);

    let _fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let handler = Arc::new(EventHandler::new());
    let mut subscriber = handler.subscribe();

    let mut options = setup_client_options;
    options.app_name = Some("SDAMMinPoolSizeErrorTest".to_string());
    options.min_pool_size = Some(10);
    options.server_selection_timeout = Some(Duration::from_millis(1000));
    options.heartbeat_freq = Some(Duration::from_secs(10));
    options.cmap_event_handler = Some(handler.clone());
    let client = Client::with_options(options).expect("client creation should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(2000), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    // Wait a little while for the server to be marked as Unknown.
    // Once we have SDAM monitoring, this wait can be removed and be replaced
    // with another event waiting.
    RUNTIME.delay_for(Duration::from_millis(750)).await;

    let ping_err = client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .expect_err("ping should fail");
    assert!(
        matches!(ping_err.kind, ErrorKind::ServerSelectionError { .. }),
        "Expected to fail due to server selection timing out, instead got: {:?}",
        ping_err.kind
    );

    // disable the fail point, then the pool should become ready again
    drop(_fp_guard);

    client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .expect("ping should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(10), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");
}

/// Based on the auth-error.yml SDAM integration test
/// TODO: RUST-360 run this via the integration test runner
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn auth_error() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;
    if !VersionReq::parse(">= 4.4.0")
        .unwrap()
        .matches(&setup_client.server_version)
    {
        println!("skipping auth_error test due to server not supporting appName failCommand");
        return;
    }

    if !setup_client.auth_enabled() {
        println!("skipping auth_error test due to auth not being enabled");
        return;
    }

    setup_client
        .init_db_and_coll("auth_error", "auth_error")
        .await
        .insert_many(vec![doc! { "_id": 1 }, doc! { "_id": 2 }], None)
        .await
        .unwrap();

    let fp_options = FailCommandOptions::builder()
        .app_name("authErrorTest".to_string())
        .error_code(18)
        .build();
    let failpoint = FailPoint::fail_command(&["saslContinue"], FailPointMode::Times(1), fp_options);

    let _fp_guard = setup_client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    let handler = Arc::new(EventHandler::new());
    let mut subscriber = handler.subscribe();

    let mut options = setup_client_options.clone();
    options.app_name = Some("authErrorTest".to_string());
    options.retry_writes = Some(false);
    options.cmap_event_handler = Some(handler.clone());
    options.command_event_handler = Some(handler.clone());
    let client = Client::with_options(options).expect("client creation should succeed");

    let coll = client.database("auth_error").collection("auth_error");
    let auth_err = coll
        .insert_many(vec![doc! { "_id": 3 }, doc! { "_id": 4 }], None)
        .await
        .expect_err("insert should fail");
    assert!(matches!(
        auth_err.kind,
        ErrorKind::AuthenticationError { .. }
    ));

    subscriber
        .wait_for_event(Duration::from_millis(2000), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    // Wait a little while for the server to be marked as Unknown.
    // Once we have SDAM monitoring, this wait can be removed and be replaced
    // with another event waiting.
    // TODO: RUST-232 replace this with monitoring.
    RUNTIME.delay_for(Duration::from_millis(750)).await;

    coll.insert_many(vec![doc! { "_id": 5 }, doc! { "_id": 6 }], None)
        .await
        .expect("insert should succeed");

    // assert that no more pool cleared events were emitted
    assert!(
        subscriber
            .wait_for_event(Duration::from_millis(100), |event| {
                matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
            })
            .await
            .is_none(),
        "no more pool cleared events"
    );

    let command_events = handler.get_command_started_events(&["insert"]);
    assert_eq!(command_events.len(), 1); // the first insert was never actually attempted
    assert_eq!(
        command_events[0].command.get_array("documents").unwrap(),
        &vec![bson!({ "_id": 5 }), bson!({ "_id": 6 })]
    );
}
