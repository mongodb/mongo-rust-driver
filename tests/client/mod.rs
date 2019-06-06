use bson::Bson;
use mongodb::Client;

use crate::MONGODB_URI;

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
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();
    let db = client.database("admin");
    let result = db.run_command(doc! { "currentOp": 1 }, None).unwrap();

    let in_prog = match result.get("inprog") {
        Some(Bson::Array(in_prog)) => in_prog,
        _ => panic!("no `inprog` array found in response to `currentOp`"),
    };

    let metadata: Metadata = bson::from_bson(in_prog[0].clone()).unwrap();
    assert_eq!(metadata.client.driver.name, "mrd");
}

#[test]
fn list_databases() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();

    let expected_dbs = &["list_databases1", "list_databases2", "list_databases3"];

    for name in expected_dbs {
        client.database(name).drop().unwrap();
    }

    let prev_dbs = client.list_databases(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs
            .iter()
            .any(|doc| doc.get("name") == Some(&Bson::String(name.to_string()))));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = client.list_databases(None).unwrap();
    let new_dbs: Vec<_> = new_dbs
        .into_iter()
        .filter(|doc| match doc.get("name") {
            Some(&Bson::String(ref name)) => expected_dbs.contains(&name.as_str()),
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
fn list_database_names() {
    let client = Client::with_uri(MONGODB_URI.as_str()).unwrap();

    let expected_dbs = &[
        "list_database_names1",
        "list_database_names2",
        "list_database_names3",
    ];

    for name in expected_dbs {
        client.database(name).drop().unwrap();
    }

    let prev_dbs = client.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert!(!prev_dbs.iter().any(|db_name| db_name == name));

        let db = client.database(name);

        db.collection("foo")
            .insert_one(doc! { "x": 1 }, None)
            .unwrap();
    }

    let new_dbs = client.list_database_names(None).unwrap();

    for name in expected_dbs {
        assert_eq!(new_dbs.iter().filter(|db_name| db_name == name).count(), 1);
    }
}
