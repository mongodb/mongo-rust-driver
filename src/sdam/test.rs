use std::{sync::Arc, time::Duration};

use bson::doc;
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

// TODO: RUST-232 update this test to incorporate SDAM events
#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
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
        true,
    )
    .await;

    if !VersionReq::parse(">= 4.2.9")
        .unwrap()
        .matches(client.server_version.as_ref().unwrap())
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
#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_min_pool_size_error() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut setup_client_options = CLIENT_OPTIONS.clone();
    setup_client_options.hosts.drain(1..);
    setup_client_options.direct_connection = Some(true);

    let setup_client = TestClient::with_options(Some(setup_client_options.clone()), true).await;

    if !VersionReq::parse(">= 4.9.0")
        .unwrap()
        .matches(setup_client.server_version.as_ref().unwrap())
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
        matches!(
            ping_err.kind.as_ref(),
            ErrorKind::ServerSelectionError { .. }
        ),
        "Expected to fail due to server selection timing out, instead got: {:?}",
        ping_err.kind.as_ref()
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
