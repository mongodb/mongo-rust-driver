use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bson::{bson, doc};
use semver::VersionReq;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    error::{Error, ErrorKind},
    hello::{LEGACY_HELLO_COMMAND_NAME, LEGACY_HELLO_COMMAND_NAME_LOWERCASE},
    runtime,
    test::{
        log_uncaptured,
        CmapEvent,
        Event,
        EventClient,
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
async fn min_heartbeat_frequency() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    if setup_client_options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping min_heartbeat_frequency test due to load-balanced topology");
        return;
    }
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !setup_client.supports_fail_command() {
        log_uncaptured(
            "skipping min_heartbeat_frequency test due to server not supporting fail points",
        );
        return;
    }

    if setup_client.server_version_lt(4, 9) {
        log_uncaptured(
            "skipping min_heartbeat_frequency test due to server version being less than 4.9",
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

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_pool_management() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    if options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping sdam_pool_management test due to load-balanced topology");
        return;
    }
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
        FailPointMode::Times(1),
        fp_options,
    );

    let _fp_guard = client
        .enable_failpoint(failpoint, None)
        .await
        .expect("enabling failpoint should succeed");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatFailed(_)))
        })
        .await
        .expect("should see server heartbeat failed event");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Cmap(CmapEvent::PoolReady(_)))
        })
        .await
        .expect("should see pool ready event");

    subscriber
        .wait_for_event(Duration::from_millis(1000), |event| {
            matches!(event, Event::Sdam(SdamEvent::ServerHeartbeatSucceeded(_)))
        })
        .await
        .expect("should see server heartbeat succeeded event");
}

// prose version of minPoolSize-error.yml SDAM integration test
// TODO: RUST-560 replace this test with the spec runner
#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_min_pool_size_error() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    if setup_client_options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping sdam_min_pool_size_error test due to load-balanced topology");
        return;
    }
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone())).await;

    if !VersionReq::parse(">= 4.9.0")
        .unwrap()
        .matches(&setup_client.server_version)
    {
        log_uncaptured(
            "skipping sdam_pool_management test due to server not supporting appName failCommand",
        );
        return;
    }

    let fp_options = FailCommandOptions::builder()
        .app_name("SDAMMinPoolSizeErrorTest".to_string())
        .error_code(1234)
        .build();
    let failpoint = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::Skip(3),
        fp_options,
    );

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
            matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    // Wait a little while for the server to be marked as Unknown.
    // Once we have SDAM monitoring, this wait can be removed and be replaced
    // with another event waiting.
    runtime::delay_for(Duration::from_millis(750)).await;

    let ping_err = client
        .database("admin")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .expect_err("ping should fail");
    assert!(
        matches!(*ping_err.kind, ErrorKind::ServerSelection { .. }),
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
            matches!(event, Event::Cmap(CmapEvent::PoolReady(_)))
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
        log_uncaptured("skipping auth_error test due to server not supporting appName failCommand");
        return;
    }

    if !setup_client.auth_enabled() {
        log_uncaptured("skipping auth_error test due to auth not being enabled");
        return;
    }

    if setup_client.is_load_balanced() {
        log_uncaptured("skipping auth_error test due to load balanced topology");
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
    options.sdam_event_handler = Some(handler.clone());
    let client = Client::with_options(options).expect("client creation should succeed");

    let coll = client.database("auth_error").collection("auth_error");
    let auth_err = coll
        .insert_many(vec![doc! { "_id": 3 }, doc! { "_id": 4 }], None)
        .await
        .expect_err("insert should fail");
    assert!(matches!(*auth_err.kind, ErrorKind::Authentication { .. }));

    // collect the events as the order is non-deterministic
    let mut seen_clear = false;
    let mut seen_mark_unknown = false;
    let events = subscriber
        .collect_events(Duration::from_secs(1), |event| match event {
            Event::Sdam(SdamEvent::ServerDescriptionChanged(event))
                if event.is_marked_unknown_event() =>
            {
                seen_mark_unknown = true;
                true
            }
            Event::Cmap(CmapEvent::PoolCleared(_)) => {
                seen_clear = true;
                true
            }
            _ => false,
        })
        .await;

    // verify we saw exactly one of both types of events
    assert!(
        seen_clear && seen_mark_unknown && events.len() == 2,
        "expected one marked unknown event, one pool cleared event, instead got {:#?}",
        events
    );

    coll.insert_many(vec![doc! { "_id": 5 }, doc! { "_id": 6 }], None)
        .await
        .expect("insert should succeed");

    // assert that no more pool cleared events were emitted
    assert!(
        subscriber
            .wait_for_event(Duration::from_millis(100), |event| {
                matches!(event, Event::Cmap(CmapEvent::PoolCleared(_)))
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

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn hello_ok_true() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
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

    let handler = Arc::new(EventHandler::new());
    let mut subscriber = handler.subscribe();

    let mut options = setup_client_options.clone();
    options.sdam_event_handler = Some(handler.clone());
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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn repl_set_name_mismatch() -> crate::error::Result<()> {
    let _guard = LOCK.run_concurrently().await;

    let client = TestClient::new().await;
    if !client.is_replica_set() {
        log_uncaptured("skipping repl_set_name_mismatch due to non-replica set topology");
        return Ok(());
    }

    let mut options = CLIENT_OPTIONS.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.repl_set_name = Some("invalid".to_string());
    let client = Client::with_options(options)?;
    let result = client.list_database_names(None, None).await;
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
