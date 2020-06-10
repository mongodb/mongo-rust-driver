use serde::Deserialize;

use crate::{
    bson::doc,
    cmap::{options::ConnectionPoolOptions, Command, ConnectionPool},
    selection_criteria::ReadPreference,
    test::{CLIENT_OPTIONS, LOCK},
};

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
    let _guard = LOCK.run_concurrently().await;

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
