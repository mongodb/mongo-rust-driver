use serde::Deserialize;
use tokio::sync::RwLockReadGuard;

use super::event::{Event, EventHandler};
use crate::{
    bson::doc,
    cmap::{options::ConnectionPoolOptions, Command, ConnectionPool},
    runtime::AsyncJoinHandle,
    selection_criteria::ReadPreference,
    test::{TestClient, CLIENT_OPTIONS, LOCK},
    RUNTIME,
};
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

    // stall creating connections for a while
    let failpoint = doc! {
        "configureFailPoint": "failCommand",
        "mode": "alwaysOn",
        "data": { "failCommands": [ "isMaster" ], "blockConnection": true, "blockTimeMS": 1000 }
    };
    let client = TestClient::new().await;
    client
        .database("admin")
        .run_command(failpoint, None)
        .await
        .expect("failpoint should succeed");

    let handler = Arc::new(EventHandler::new());
    let options = ConnectionPoolOptions::builder()
        .event_handler(handler.clone() as Arc<dyn crate::cmap::CmapEventHandler>)
        .build();
    let pool = ConnectionPool::new(
        CLIENT_OPTIONS.hosts[0].clone(),
        Default::default(),
        Some(options),
    );

    let mut tasks: Vec<AsyncJoinHandle<()>> = Default::default();
    for _ in 0..3 {
        let pool_clone = pool.clone();
        tasks.push(
            RUNTIME
                .spawn(async move {
                    pool_clone.check_out().await.unwrap();
                })
                .unwrap(),
        );
    }

    while let Some(handle) = tasks.pop() {
        let _ = handle.await;
    }

    // ensure all three ConnectionCreatedEvents were emitted before one ConnectionReadyEvent.
    let mut events = handler.events.write().unwrap();
    let mut consecutive_creations = 0;
    while let Some(event) = events.pop_front() {
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
