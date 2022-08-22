use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use bson::doc;
use semver::VersionReq;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    error::{Error, ErrorKind},
    hello::{LEGACY_HELLO_COMMAND_NAME, LEGACY_HELLO_COMMAND_NAME_LOWERCASE},
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

    let mut setup_client_options = CLIENT_OPTIONS.get().await.clone();
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

    let mut options = CLIENT_OPTIONS.get().await.clone();
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

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn hello_ok_true() {
    let _guard: RwLockReadGuard<_> = LOCK.run_concurrently().await;

    let mut setup_client_options = CLIENT_OPTIONS.get().await.clone();
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

    let mut options = CLIENT_OPTIONS.get().await.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.repl_set_name = Some("invalid".to_string());
    options.server_selection_timeout = Some(Duration::from_secs(5));
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
