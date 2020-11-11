use std::time::Duration;

use tokio::sync::RwLockWriteGuard;

use crate::test::{
    CmapEvent,
    Event,
    EventClient,
    FailCommandOptions,
    FailPoint,
    FailPointMode,
    CLIENT_OPTIONS,
    LOCK,
};

#[cfg_attr(feature = "tokio-runtime", tokio::test(threaded_scheduler))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn sdam_pool_management() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    options.hosts.drain(1..);
    options.direct_connection = Some(true);
    options.app_name = Some("SDAMPoolManagementTest".to_string());
    options.heartbeat_freq = Some(Duration::from_millis(50));
    println!("{:?}", options.hosts);

    let client = EventClient::with_additional_options(
        Some(options),
        Some(Duration::from_millis(50)),
        None,
        true,
    )
    .await;

    if !client.supports_fail_command().await {
        println!("skipping sdam_pool_management test due to server not supporting fail points");
        return;
    }

    let mut subscriber = client.subscribe_to_events();

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
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolCleared(_)))
        })
        .await
        .expect("should see pool cleared event");

    subscriber
        .wait_for_event(Duration::from_millis(500), |event| {
            matches!(event, Event::CmapEvent(CmapEvent::ConnectionPoolReady(_)))
        })
        .await
        .expect("should see pool ready event");
}
