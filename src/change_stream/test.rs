use bson::{Bson, Document};

use crate::{
    change_stream::document::ChangeStreamDocument, error::Result, options::ChangeStreamOptions,
    topology::description::TopologyType, Client,
};

#[derive(Debug, Deserialize)]
struct TestFile {
    collection_name: String,
    database_name: String,
    collection2_name: String,
    database2_name: String,
    tests: Vec<TestCase>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestCase {
    description: String,
    min_server_version: String,
    max_server_version: Option<String>,
    fail_point: Option<Document>,
    target: Target,
    topology: Vec<Topology>,
    change_stream_pipeline: Vec<Document>,
    change_stream_options: ChangeStreamOptions,
    operations: Vec<Document>,
    expectations: Vec<Document>,
    result: Outcome,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Target {
    Collection,
    Database,
    Client,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
enum Topology {
    Single,
    ReplicaSet,
    Sharded,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
enum Outcome {
    Error(i32),
    Success(Vec<ChangeStreamDocument>),
}

fn matches(expected: impl Into<Bson>, actual: impl Into<Bson>) -> bool {
    let mut expected_bson = expected.into();
    let actual_bson = actual.into();

    if expected_bson == bson::to_bson("42").unwrap() {
        return true;
    }

    expected_bson == actual_bson
}

fn run_test(
    mut test_case: TestCase,
    test_file: TestFile,
    global_client: Client,
) -> Result<Vec<Document>> {
    test_case.description = test_case.description.replace('$', "%");

    let topology = match global_client.get_topology_type() {
        TopologyType::Single => Topology::Single,
        TopologyType::ReplicaSetNoPrimary | TopologyType::ReplicaSetWithPrimary => {
            Topology::ReplicaSet
        }
        TopologyType::Sharded => Topology::Sharded,
    };
    if !test_case.topology.contains(&topology) {
        return Ok(Vec::new());
    }

    global_client.database(&test_file.database_name).drop()?;
    global_client.database(&test_file.database2_name).drop()?;

    let db = global_client.database(&test_file.database_name);
    let db2 = global_client.database(&test_file.database2_name);
    db.create_collection(&test_file.collection_name, None)?;
    db2.create_collection(&test_file.collection2_name, None)?;
    let coll = db.collection(&test_file.collection_name);
    let coll2 = db.collection(&test_file.collection2_name);

    if let Some(fail_point) = test_case.fail_point {
        let admin_db = global_client.database("admin");
        admin_db.run_command(fail_point, None)?;
    }

    let client = Client::with_uri_str("mongodb://localhost:27017")?;

    // TODO: Begin monitoring all APM events for client

    // TODO: Change to correct collection and database targets
    let change_stream = match test_case.target {
        Collection => coll.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
        Database => db.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
        Client => client.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
    };

    for operation in test_case.operations {
        global_client
            .database("admin")
            .run_command(operation, None)?;
    }

    let changes = Vec::new();
    match test_case.result {
        Outcome::Error(_) => {
            change_stream.next().transpose()?;
        }
        Outcome::Success(_) => {
            for change in change_stream {
                changes.push(change?);
            }
        }
    }

    Ok(changes)
}

fn run_change_stream_test(test_file: TestFile) {
    let global_client = Client::with_uri_str("mongodb://localhost:27017").unwrap();

    for mut test_case in test_file.tests {
        match run_test(test_case, test_file, global_client) {
            Err(e) => match test_case.result {
                Outcome::Error(code) => assert!(matches(code, e.0)),
                Outcome::Success(_) => panic!(&test_case.description),
            },
            Ok(changes) => match test_case.result {
                Outcome::Error(_) => panic!(&test_case.description),
                Outcome::Success(docs) => {
                    for pair in docs.iter().zip(changes.iter()) {
                        assert!(matches(pair.0, pair.1));
                    }
                }
            },
        }

        // TODO: Assert expectations
    }

    global_client.database(&test_file.database_name).drop();
    global_client.database(&test_file.database2_name).drop();
}

#[test]
fn run() {
    crate::tests::spec::test(&["change-streams"], run_change_stream_test);
}
