use bson::{bson, doc};
use serde::Deserialize;

use crate::read_preference::ReadPreference;
use crate::{
    cmap::{conn::command::Command, options::ConnectionPoolOptions, ConnectionPool},
    test::CLIENT_OPTIONS,
};

#[derive(Debug, Deserialize)]
struct ListDatabasesResponse {
    databases: Vec<DatabaseEntry>,
}

#[derive(Debug, Deserialize)]
struct DatabaseEntry {
    name: String,
}

#[test]
fn acquire_connection_and_send_command() {
    let pool_options = ConnectionPoolOptions::from_client_options(&CLIENT_OPTIONS);

    let pool = ConnectionPool::new(CLIENT_OPTIONS.hosts[0].clone(), Some(pool_options), None);
    let mut connection = pool.check_out().unwrap();

    let body = doc! { "listDatabases": 1 };
    let read_pref = ReadPreference::PrimaryPreferred {
        tag_sets: None,
        max_staleness: None,
    };
    let cmd = Command::new_read("listDatabases", "admin".to_string(), Some(read_pref), body);
    let response = connection.send_command(cmd).unwrap();

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
