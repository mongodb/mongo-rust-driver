use bson::{bson, doc, Bson};
use serde::Deserialize;

use crate::{
    cmap::{options::ConnectionPoolOptions, ConnectionPool},
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
fn acquire_connection_and_run_operation() {
    let pool_options = ConnectionPoolOptions::from_client_options(&CLIENT_OPTIONS);

    let pool = ConnectionPool::new(CLIENT_OPTIONS.hosts[0].clone(), Some(pool_options), None);
    let mut connection = pool.check_out().unwrap();

    let doc = connection
        .execute_operation(doc! {
            "listDatabases": 1,
            "$db": "admin",
            "$readPreference": { "mode": "primaryPreferred" },
        })
        .unwrap();

    let response: ListDatabasesResponse = bson::from_bson(Bson::Document(dbg!(doc))).unwrap();

    let names: Vec<_> = response
        .databases
        .into_iter()
        .map(|entry| entry.name)
        .collect();

    assert!(names.iter().any(|name| name == "admin"));
    assert!(names.iter().any(|name| name == "config"));
}
