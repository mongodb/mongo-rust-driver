use serde::Deserialize;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{
    event::{Event, EventHandler},
    EVENT_TIMEOUT,
};
use crate::{
    bson::doc,
    cmap::{options::ConnectionPoolOptions, Command, ConnectionPool},
    event::cmap::{CmapEventHandler, ConnectionClosedReason},
    sdam::ServerUpdateSender,
    selection_criteria::ReadPreference,
    test::{FailCommandOptions, FailPoint, FailPointMode, TestClient, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};
use semver::VersionReq;
use std::{sync::Arc, time::Duration};

#[derive(Debug, Deserialize)]
struct ListDatabasesResponse {
    databases: Vec<DatabaseEntry>,
}

#[derive(Debug, Deserialize)]
struct DatabaseEntry {
    name: String,
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn acquire_connection_and_send_command() {
    let _guard: RwLockReadGuard<()> = LOCK.run_concurrently().await;

    let client_options = CLIENT_OPTIONS.clone();
    let mut pool_options = ConnectionPoolOptions::from_client_options(&client_options);
    pool_options.ready = Some(true);

    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        Default::default(),
        ServerUpdateSender::channel().0,
        Some(pool_options),
    );
    let mut connection = pool.check_out().await.unwrap();

    let body = doc! { "listDatabases": 1 };
    let read_pref = ReadPreference::PrimaryPreferred {
        options: Default::default(),
    };
    let mut cmd = Command::new("listDatabases".to_string(), "admin".to_string(), body);
    cmd.set_read_preference(read_pref);
    if let Some(server_api) = client_options.server_api.as_ref() {
        cmd.set_server_api(server_api);
    }

    let response = connection.send_command(cmd, None).await.unwrap();

    assert!(response.is_success());

    let response: ListDatabasesResponse = response.body().unwrap();

    let names: Vec<_> = response
        .databases
        .into_iter()
        .map(|entry| entry.name)
        .collect();

    assert!(names.iter().any(|name| name == "admin"));
    assert!(names.iter().any(|name| name == "config"));
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn concurrent_connections() {
    let _guard = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    options.direct_connection = Some(true);
    options.hosts.drain(1..);

    let client = TestClient::with_options(Some(options)).await;
    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&client.server_version) {
        println!(
            "skipping concurrent_connections test due to server not supporting failpoint option"
        );
        return;
    }

    // stall creating connections for a while
    let failpoint = doc! {
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": { "failCommands": [ "isMaster" ], "blockConnection": true, "blockTimeMS": 1000 }
    };
    client
        .database("admin")
        .run_command(failpoint, None)
        .await
        .expect("failpoint should succeed");

    let handler = Arc::new(EventHandler::new());
    let client_options = CLIENT_OPTIONS.clone();
    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.event_handler = Some(handler.clone() as Arc<dyn crate::cmap::CmapEventHandler>);
    options.ready = Some(true);

    let pool = ConnectionPool::new(
        CLIENT_OPTIONS.hosts[0].clone(),
        Default::default(),
        ServerUpdateSender::channel().0,
        Some(options),
    );

    let tasks = (0..2).map(|_| {
        let pool_clone = pool.clone();
        RUNTIME
            .spawn(async move {
                pool_clone.check_out().await.unwrap();
            })
            .unwrap()
    });
    futures::future::join_all(tasks).await;

    {
        // ensure all three ConnectionCreatedEvents were emitted before one ConnectionReadyEvent.
        let events = handler.events.read().unwrap();
        let mut consecutive_creations = 0;
        for event in events.iter() {
            match event {
                Event::ConnectionCreated(_) => {
                    consecutive_creations += 1;
                }
                Event::ConnectionReady(_) => {
                    if consecutive_creations < 2 {
                        panic!("connections not created concurrently");
                    }
                }
                _ => (),
            }
        }
    }

    // clear the fail point
    client
        .database("admin")
        .run_command(
            doc! { "configureFailPoint": "failCommand", "mode": "off" },
            None,
        )
        .await
        .expect("disabling fail point should succeed");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn connection_error_during_establishment() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut client_options = CLIENT_OPTIONS.clone();
    client_options.heartbeat_freq = Duration::from_secs(300).into(); // high so that monitors dont trip failpoint
    client_options.hosts.drain(1..);
    client_options.direct_connection = Some(true);
    client_options.repl_set_name = None;

    let client = TestClient::with_options(Some(client_options.clone())).await;
    if !client.supports_fail_command().await {
        println!(
            "skipping {} due to failCommand not being supported",
            function_name!()
        );
        return;
    }

    let options = FailCommandOptions::builder().error_code(1234).build();
    let failpoint = FailPoint::fail_command(&["isMaster"], FailPointMode::Times(10), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let handler = Arc::new(EventHandler::new());
    let mut subscriber = handler.subscribe();

    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.ready = Some(true);
    options.event_handler = Some(handler.clone() as Arc<dyn crate::cmap::CmapEventHandler>);
    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        Default::default(),
        ServerUpdateSender::channel().0,
        Some(options),
    );

    pool.check_out().await.expect_err("check out should fail");

    subscriber
        .wait_for_event(EVENT_TIMEOUT, |e| match e {
            Event::ConnectionClosed(event) => {
                event.connection_id == 1 && event.reason == ConnectionClosedReason::Error
            }
            _ => false,
        })
        .await
        .expect("closed event with error reason should have been seen");
}

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
#[function_name::named]
async fn connection_error_during_operation() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let mut options = CLIENT_OPTIONS.clone();
    let handler = Arc::new(EventHandler::new());
    options.cmap_event_handler = Some(handler.clone() as Arc<dyn CmapEventHandler>);
    options.hosts.drain(1..);
    options.max_pool_size = Some(1);

    let client = TestClient::with_options(options.into()).await;
    if !client.supports_fail_command().await {
        println!(
            "skipping {} due to failCommand not being supported",
            function_name!()
        );
        return;
    }

    let options = FailCommandOptions::builder().close_connection(true).build();
    let failpoint = FailPoint::fail_command(&["ping"], FailPointMode::Times(10), Some(options));
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let mut subscriber = handler.subscribe();

    client
        .database("test")
        .run_command(doc! { "ping": 1 }, None)
        .await
        .expect_err("ping should fail due to fail point");

    subscriber
        .wait_for_event(EVENT_TIMEOUT, |e| match e {
            Event::ConnectionClosed(event) => {
                event.connection_id == 1 && event.reason == ConnectionClosedReason::Error
            }
            _ => false,
        })
        .await
        .expect("closed event with error reason should have been seen");
}
