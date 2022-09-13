use serde::Deserialize;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use super::{
    event::{Event, EventHandler},
    EVENT_TIMEOUT,
};
use crate::{
    bson::{doc, Document},
    cmap::{
        establish::{ConnectionEstablisher, EstablisherOptions},
        options::ConnectionPoolOptions,
        Command, ConnectionPool,
    },
    event::cmap::{CmapEventHandler, ConnectionClosedReason},
    hello::LEGACY_HELLO_COMMAND_NAME,
    operation::CommandResponse,
    runtime,
    sdam::TopologyUpdater,
    selection_criteria::ReadPreference,
    test::{
        log_uncaptured, FailCommandOptions, FailPoint, FailPointMode, SdamEvent, TestClient,
        CLIENT_OPTIONS, LOCK,
    },
    Client,
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

    let client_options = CLIENT_OPTIONS.get().await.clone();
    let mut pool_options = ConnectionPoolOptions::from_client_options(&client_options);
    pool_options.ready = Some(true);

    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        ConnectionEstablisher::new(
            Default::default(),
            EstablisherOptions::from_client_options(&client_options),
        )
        .unwrap(),
        TopologyUpdater::channel().0,
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
    let doc_response: CommandResponse<Document> = response.body().unwrap();

    assert!(doc_response.is_success());

    let response: ListDatabasesResponse = bson::from_document(doc_response.body).unwrap();

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

    let mut options = CLIENT_OPTIONS.get().await.clone();
    if options.load_balanced.unwrap_or(false) {
        log_uncaptured("skipping concurrent_connections test due to load-balanced topology");
        return;
    }
    options.direct_connection = Some(true);
    options.hosts.drain(1..);

    let client = TestClient::with_options(Some(options)).await;
    let version = VersionReq::parse(">= 4.2.9").unwrap();
    // blockConnection failpoint option only supported in 4.2.9+.
    if !version.matches(&client.server_version) {
        log_uncaptured(
            "skipping concurrent_connections test due to server not supporting failpoint option",
        );
        return;
    }

    // stall creating connections for a while
    let failpoint = doc! {
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": { "failCommands": [LEGACY_HELLO_COMMAND_NAME, "hello"], "blockConnection": true, "blockTimeMS": 1000 }
    };
    client
        .database("admin")
        .run_command(failpoint, None)
        .await
        .expect("failpoint should succeed");

    let handler = Arc::new(EventHandler::new());
    let client_options = CLIENT_OPTIONS.get().await.clone();
    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.cmap_event_handler = Some(handler.clone() as Arc<dyn crate::cmap::CmapEventHandler>);
    options.ready = Some(true);

    let pool = ConnectionPool::new(
        CLIENT_OPTIONS.get().await.hosts[0].clone(),
        ConnectionEstablisher::new(
            Default::default(),
            EstablisherOptions::from_client_options(&client_options),
        )
        .unwrap(),
        TopologyUpdater::channel().0,
        Some(options),
    );

    let tasks = (0..2).map(|_| {
        let pool_clone = pool.clone();
        runtime::spawn(async move {
            pool_clone.check_out().await.unwrap();
        })
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
                    assert!(
                        consecutive_creations >= 2,
                        "connections not created concurrently"
                    );
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

    let mut client_options = CLIENT_OPTIONS.get().await.clone();
    if client_options.load_balanced.unwrap_or(false) {
        log_uncaptured(
            "skipping connection_error_during_establishment test due to load-balanced topology",
        );
        return;
    }
    client_options.heartbeat_freq = Duration::from_secs(300).into(); // high so that monitors dont trip failpoint
    client_options.hosts.drain(1..);
    client_options.direct_connection = Some(true);
    client_options.repl_set_name = None;

    let client = TestClient::with_options(Some(client_options.clone())).await;
    if !client.supports_fail_command() {
        log_uncaptured(format!(
            "skipping {} due to failCommand not being supported",
            function_name!()
        ));
        return;
    }

    let options = FailCommandOptions::builder().error_code(1234).build();
    let failpoint = FailPoint::fail_command(
        &[LEGACY_HELLO_COMMAND_NAME, "hello"],
        FailPointMode::Times(10),
        Some(options),
    );
    let _fp_guard = client.enable_failpoint(failpoint, None).await.unwrap();

    let handler = Arc::new(EventHandler::new());
    let mut subscriber = handler.subscribe();

    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.ready = Some(true);
    options.cmap_event_handler = Some(handler.clone() as Arc<dyn crate::cmap::CmapEventHandler>);
    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        ConnectionEstablisher::new(
            Default::default(),
            EstablisherOptions::from_client_options(&client_options),
        )
        .unwrap(),
        TopologyUpdater::channel().0,
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

    let mut options = CLIENT_OPTIONS.get().await.clone();
    let handler = Arc::new(EventHandler::new());
    options.cmap_event_handler = Some(handler.clone() as Arc<dyn CmapEventHandler>);
    options.hosts.drain(1..);
    options.max_pool_size = Some(1);

    let client = TestClient::with_options(options.into()).await;
    if !client.supports_fail_command() {
        log_uncaptured(format!(
            "skipping {} due to failCommand not being supported",
            function_name!()
        ));
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

#[cfg_attr(feature = "tokio-runtime", tokio::test(flavor = "multi_thread"))]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn connect_timeout() {
    let _guard: RwLockWriteGuard<_> = LOCK.run_exclusively().await;

    let setup_client = TestClient::new().await;
    if setup_client.server_version_lt(6, 0) {
        log_uncaptured("skipping connect_timeout test due to transportLayerASIOhangBeforeAccept failpoint not being supported",
);
        return;
    }

    let mut options = CLIENT_OPTIONS.get().await.clone();
    let handler = Arc::new(crate::test::EventHandler::new());
    options.hosts.drain(1..);
    options.connect_timeout = Some(Duration::from_millis(100));
    options.sdam_event_handler = Some(handler.clone());
    options.command_event_handler = Some(handler.clone());
    options.cmap_event_handler = Some(handler.clone());
    options.retry_reads = Some(false);
    options.retry_writes = Some(false);

    let mut subscriber = handler.subscribe();
    let client = Client::with_options(options).unwrap();

    subscriber
        .wait_for_event(Duration::from_secs(5), |event| match event {
            crate::test::Event::Sdam(SdamEvent::ServerDescriptionChanged(e)) => {
                !e.previous_description.server_type().is_available()
                    && e.new_description.server_type().is_available()
            },
            _ => false
        })
        .await
        .expect("should discover server");

    let options = FailCommandOptions::builder().close_connection(true).build();
    let failpoint = FailPoint::new(
        "transportLayerASIOhangBeforeAccept",
        FailPointMode::AlwaysOn,
        None,
    );
    let _fp_guard = setup_client.enable_failpoint(failpoint, None).await.unwrap();

    let err = runtime::timeout(
        Duration::from_millis(500),
        client
            .database("test")
            .run_command(doc! { "ping": 1 }, None),
    )
    .await;

    subscriber.collect_events(Duration::from_secs(5), |event| {
        println!("{:#?}", event);
        true
    }).await;

    // .expect("should hit connecttimeoutms")
    // .expect_err("ping should fail due to fail point");

    // assert!(err.is_network_timeout());
}
