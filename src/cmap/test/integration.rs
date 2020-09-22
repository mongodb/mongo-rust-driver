use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use super::event::{Event, EventHandler};
use crate::{
    bson::doc,
    cmap::{options::ConnectionPoolOptions, Command, ConnectionPool},
    selection_criteria::ReadPreference,
    test::{TestClient, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};
use semver::VersionReq;
use std::sync::Arc;

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
    let pool_options = ConnectionPoolOptions::from_client_options(&client_options);

    let pool = ConnectionPool::new(
        client_options.hosts[0].clone(),
        Default::default(),
        Some(pool_options),
    );
    let mut connection = pool.check_out().await.unwrap();

    let body = doc! { "listDatabases": 1 };
    let read_pref = ReadPreference::PrimaryPreferred {
        options: Default::default(),
    };
    let cmd = Command::new_read(
        "listDatabases".to_string(),
        "admin".to_string(),
        Some(read_pref),
        body,
    );
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

    let client = TestClient::new().await;
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
    let pool = ConnectionPool::new(
        CLIENT_OPTIONS.hosts[0].clone(),
        Default::default(),
        Some(options),
    );

    let tasks = (0..3).map(|_| {
        let pool_clone = pool.clone();
        RUNTIME
            .spawn(async move {
                pool_clone.check_out().await.unwrap();
            })
            .unwrap()
    });
    futures::future::join_all(tasks).await;

    // ensure all three ConnectionCreatedEvents were emitted before one ConnectionReadyEvent.
    let events = handler.events.read().unwrap();
    let mut consecutive_creations = 0;
    for event in events.iter() {
        match event {
            Event::ConnectionCreated(_) => {
                consecutive_creations += 1;
            }
            Event::ConnectionReady(_) => {
                if consecutive_creations < 3 {
                    panic!("connections not created concurrently");
                }
            }
            _ => (),
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
