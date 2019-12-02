use bson::{bson, doc, Bson};
use serde::Deserialize;

use crate::{CLIENT, LOCK};

#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(rename = "clientMetadata")]
    pub client: ClientMetadata,
}

#[derive(Debug, Deserialize)]
struct ClientMetadata {
    pub driver: DriverMetadata,
    pub os: OsMetadata,
}

#[derive(Debug, Deserialize)]
struct DriverMetadata {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Deserialize)]
struct OsMetadata {
    #[serde(rename = "type")]
    pub os_type: String,
    pub architecture: String,
}

// This test currently doesn't pass on replica sets and sharded clusters consistently due to
// `currentOp` sometimes detecting heartbeats between the server. Eventually we can test this using
// APM or coming up with something more clever, but for now, we're just disabling it.
//
// #[test]
#[allow(unused)]
fn metadata_sent_in_handshake() {
    let db = CLIENT.database("admin");
    let result = db.run_command(doc! { "currentOp": 1 }, None).unwrap();

    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, "mrd");
}

#[test]
#[function_name::named]
fn list_databases() {
    let _guard = LOCK.run_concurrently();

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop(None).unwrap();
    }

    let prev_dbs = CLIENT.list_databases(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs
            .iter()
            .any(|doc| doc.get("name") == Some(&Bson::String(name.to_string()))));

        let db = CLIENT.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = CLIENT.list_databases(None).unwrap();
    let new_dbs: Vec<_> = new_dbs
        .into_iter()
        .filter(|doc| match doc.get("name") {
            Some(&Bson::String(ref name)) => expected_dbs.contains(name),
            _ => false,
        })
        .collect();
    assert_eq!(new_dbs.len(), expected_dbs.len());

    for name in expected_dbs {
        let db_doc = new_dbs
            .iter()
            .find(|doc| doc.get("name") == Some(&Bson::String(name.to_string())))
            .unwrap();
        assert!(db_doc.contains_key("sizeOnDisk"));
        assert!(db_doc.contains_key("empty"));
    }
}

#[test]
#[function_name::named]
fn list_database_names() {
    let _guard = LOCK.run_concurrently();

    let expected_dbs = &[
        format!("{}1", function_name!()),
        format!("{}2", function_name!()),
        format!("{}3", function_name!()),
    ];

    for name in expected_dbs {
        CLIENT.database(name).drop(None).unwrap();
    }

    let prev_dbs = CLIENT.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|db_name| db_name == name));

        let db = CLIENT.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = CLIENT.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert_eq!(new_dbs.iter().filter(|db_name| db_name == &name).count(), 1);
    }
}
