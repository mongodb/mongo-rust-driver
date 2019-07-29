use bson::{Bson, Document};

use crate::{
    error::{ErrorKind, Result},
    options::ChangeStreamOptions,
    topology::description::TopologyType,
    Client,
};

#[derive(Debug, Deserialize)]
struct TestFile {
    collection_name: String,
    database_name: String,
    collection2_name: String,
    database2_name: String,
    tests: Vec<TestCase>,
}

#[derive(Clone, Debug, Deserialize)]
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
    operations: Vec<Operation>,
    expectations: Option<Vec<Document>>,
    result: Outcome,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Target {
    Collection,
    Database,
    Client,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Topology {
    Single,
    ReplicaSet,
    Sharded,
    Unknown,
}

#[derive(Clone, Debug, Deserialize)]
struct Operation {
    database: String,
    collection: String,
    name: String,
    arguments: Option<Document>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Outcome {
    Error { code: i32 },
    Success(Vec<Document>),
}

fn matches(expected: impl Into<Bson>, actual: impl Into<Bson>) -> bool {
    let expected_bson = expected.into();
    let actual_bson = actual.into();

    if expected_bson == bson::to_bson("42").unwrap() {
        return true;
    }

    expected_bson == actual_bson
}

fn run_test(
    mut test_case: TestCase,
    test_file: &TestFile,
    global_client: &Client,
) -> Result<Vec<Document>> {
    test_case.description = test_case.description.replace('$', "%");

    let topology = match global_client.get_topology_type() {
        TopologyType::Single => Topology::Single,
        TopologyType::ReplicaSetNoPrimary | TopologyType::ReplicaSetWithPrimary => {
            Topology::ReplicaSet
        }
        TopologyType::Sharded => Topology::Sharded,
        TopologyType::Unknown => Topology::Unknown,
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

    if let Some(fail_point) = test_case.fail_point {
        let admin_db = global_client.database("admin");
        admin_db.run_command(fail_point, None)?;
    }

    let client =
        Client::with_uri_str(option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017"))?;

    // TODO: Begin monitoring all APM events for client

    let mut change_stream = match test_case.target {
        Target::Collection => coll.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
        Target::Database => db.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
        Target::Client => client.watch(
            test_case.change_stream_pipeline,
            Some(test_case.change_stream_options),
        )?,
    };

    for operation in test_case.operations {
        global_client
            .database(&operation.database)
            .collection(&operation.collection)
            .run_command(operation.arguments, None)?;
    }

    let mut changes = Vec::new();
    match test_case.result {
        Outcome::Error { code: _ } => {
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
    let global_client =
        Client::with_uri_str(option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017"))
            .unwrap();

    for test_case in test_file.tests.clone() {
        let description = test_case.description.clone();
        let result = test_case.result.clone();
        match run_test(test_case, &test_file, &global_client) {
            Err(e) => match result {
                Outcome::Error { code } => match e.kind() {
                    ErrorKind::CommandError(ref inner) => {
                        assert!(matches(code, inner.code));
                    }
                    _ => panic!("{}: wrong type of error ({}) returned", &description, e),
                },
                Outcome::Success(_) => {
                    panic!("{}: unexpected error ({}) was returned", &description, e)
                }
            },
            Ok(changes) => match result {
                Outcome::Error { code } => panic!(
                    "{}: expected error (code: {}) was not returned",
                    &description, code
                ),
                Outcome::Success(docs) => {
                    for pair in docs.iter().zip(changes.iter()) {
                        assert!(matches(pair.0.clone(), pair.1.clone()));
                    }
                }
            },
        }

        // TODO: Assert expectations == command monitoring results
    }

    global_client
        .database(&test_file.database_name)
        .drop()
        .unwrap();
    global_client
        .database(&test_file.database2_name)
        .drop()
        .unwrap();
}

#[test]
fn run() {
    crate::test::run(&["change-streams"], run_change_stream_test);
}
