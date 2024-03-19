use serde::Deserialize;

use super::EVENT_TIMEOUT;
use crate::{
    bson::{doc, Document},
    cmap::{
        establish::{ConnectionEstablisher, EstablisherOptions},
        options::ConnectionPoolOptions,
        Command,
        ConnectionPool,
    },
    event::cmap::{CmapEvent, ConnectionClosedReason},
    hello::LEGACY_HELLO_COMMAND_NAME,
    operation::CommandResponse,
    runtime,
    sdam::TopologyUpdater,
    selection_criteria::ReadPreference,
    test::{
        get_client_options,
        log_uncaptured,
        util::event_buffer::EventBuffer,
        FailCommandOptions,
        FailPoint,
        FailPointMode,
        TestClient,
    },
};
use semver::VersionReq;
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct ListDatabasesResponse {
    databases: Vec<DatabaseEntry>,
}

#[derive(Debug, Deserialize)]
struct DatabaseEntry {
    name: String,
}

#[tokio::test]
async fn acquire_connection_and_send_command() {
    let client_options = get_client_options().await.clone();
    let mut pool_options = ConnectionPoolOptions::from_client_options(&client_options);
    pool_options.ready = Some(true);

    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        ConnectionEstablisher::new(EstablisherOptions::from_client_options(&client_options))
            .unwrap(),
        TopologyUpdater::channel().0,
        bson::oid::ObjectId::new(),
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

#[tokio::test]
async fn concurrent_connections() {
    let mut options = get_client_options().await.clone();
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
        .run_command(failpoint)
        .await
        .expect("failpoint should succeed");

    let buffer = EventBuffer::<CmapEvent>::new();
    let client_options = get_client_options().await.clone();
    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.cmap_event_handler = Some(buffer.handler());
    options.ready = Some(true);

    let pool = ConnectionPool::new(
        get_client_options().await.hosts[0].clone(),
        ConnectionEstablisher::new(EstablisherOptions::from_client_options(&client_options))
            .unwrap(),
        TopologyUpdater::channel().0,
        bson::oid::ObjectId::new(),
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
        let events = buffer.all();
        let mut consecutive_creations = 0;
        for event in events.iter() {
            match event {
                CmapEvent::ConnectionCreated(_) => {
                    consecutive_creations += 1;
                }
                CmapEvent::ConnectionReady(_) => {
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
        .run_command(doc! { "configureFailPoint": "failCommand", "mode": "off" })
        .await
        .expect("disabling fail point should succeed");
}

#[tokio::test(flavor = "multi_thread")]
#[function_name::named]

async fn connection_error_during_establishment() {
    let mut client_options = get_client_options().await.clone();
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

    let buffer = EventBuffer::<CmapEvent>::new();
    let mut subscriber = buffer.subscribe();

    let mut options = ConnectionPoolOptions::from_client_options(&client_options);
    options.ready = Some(true);
    options.cmap_event_handler = Some(buffer.handler());
    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        ConnectionEstablisher::new(EstablisherOptions::from_client_options(&client_options))
            .unwrap(),
        TopologyUpdater::channel().0,
        bson::oid::ObjectId::new(),
        Some(options),
    );

    pool.check_out().await.expect_err("check out should fail");

    subscriber
        .wait_for_event(EVENT_TIMEOUT, |e| match e {
            CmapEvent::ConnectionClosed(event) => {
                event.connection_id == 1 && event.reason == ConnectionClosedReason::Error
            }
            _ => false,
        })
        .await
        .expect("closed event with error reason should have been seen");
}

#[tokio::test(flavor = "multi_thread")]
#[function_name::named]

async fn connection_error_during_operation() {
    let mut options = get_client_options().await.clone();
    let buffer = EventBuffer::<CmapEvent>::new();
    options.cmap_event_handler = Some(buffer.handler());
    options.hosts.drain(1..);
    options.max_pool_size = Some(1);

    let client = TestClient::with_options(options).await;
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

    let mut subscriber = buffer.subscribe();

    client
        .database("test")
        .run_command(doc! { "ping": 1 })
        .await
        .expect_err("ping should fail due to fail point");

    subscriber
        .wait_for_event(EVENT_TIMEOUT, |e| match e {
            CmapEvent::ConnectionClosed(event) => {
                event.connection_id == 1 && event.reason == ConnectionClosedReason::Error
            }
            _ => false,
        })
        .await
        .expect("closed event with error reason should have been seen");
}
