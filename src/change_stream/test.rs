use bson::{Bson, Document};
use pretty_assertions::assert_eq;

use crate::{
    change_stream::document::{ChangeStreamEventDocument, OperationType},
    concern::ReadConcern,
    error::{ErrorKind, Result},
    options::{ChangeStreamOptions, ClientOptions},
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
    #[serde(flatten)]
    command: Command,
}

impl Operation {
    fn exec(self, client: &Client) -> Result<()> {
        self.command.exec(client, &self.database, &self.collection)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "name", content = "arguments")]
#[serde(rename_all = "camelCase")]
enum Command {
    DeleteOne {
        filter: Document,
    },
    Drop,
    InsertOne {
        document: Document,
    },
    Rename {
        to: String,
    },
    ReplaceOne {
        filter: Document,
        replacement: Document,
    },
    UpdateOne {
        filter: Document,
        update: Document,
    },
}

impl Command {
    fn exec(self, client: &Client, database: &str, collection: &str) -> Result<()> {
        match self {
            Command::DeleteOne { filter } => {
                client
                    .database(database)
                    .collection(collection)
                    .delete_one(filter, None)?;
            }
            Command::Drop => {
                client.database(database).collection(collection).drop()?;
            }
            Command::InsertOne { document } => {
                client
                    .database(database)
                    .collection(collection)
                    .insert_one(document, None)?;
            }
            Command::Rename { to } => {
                let rename_cmd = doc! {
                    "renameCollection": format!("{}.{}", database, collection),
                    "to": format!("{}.{}", database, to)
                };

                client.database("admin").run_command(rename_cmd, None)?;
            }
            Command::ReplaceOne {
                filter,
                replacement,
            } => {
                client
                    .database(database)
                    .collection(collection)
                    .find_one_and_replace(filter, replacement, None)?;
            }
            Command::UpdateOne { filter, update } => {
                client
                    .database(database)
                    .collection(collection)
                    .update_one(filter, update, None)?;
            }
        };

        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Outcome {
    Error { code: i32 },
    Success(Vec<ExpectedChangeStreamEventDocument>),
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedChangeStreamEventDocument {
    operation_type: OperationType,
    ns: Option<Document>,
    update_description: Option<ExpectedUpdateDescription>,
    #[serde(default)]
    #[serde(deserialize_with = "crate::extjson_test_helper::deserialize_extjson_doc")]
    full_document: Option<Document>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExpectedUpdateDescription {
    updated_fields: Document,
    removed_fields: Option<Vec<String>>,
}

lazy_static! {
    static ref CHANGE_STREAM_CLIENT_OPTIONS: ClientOptions = {
        let mut options = crate::test::CLIENT_OPTIONS.clone();
        options.read_concern = Some(ReadConcern::Majority);
        options
    };
}

type DocElement = (String, Bson);

fn normalize_integers((key, val): DocElement) -> DocElement {
    (
        key,
        crate::bson_util::get_int(&val)
            .map(Bson::I64)
            .unwrap_or(val),
    )
}

fn is_not_id((ref key, _): &DocElement) -> bool {
    key != "_id"
}

fn full_document(document: Option<Document>) -> Option<Document> {
    document.map(|doc| {
        crate::extjson_test_helper::flatten_extjson_document(doc)
            .filter(is_not_id)
            .map(normalize_integers)
            .collect()
    })
}

fn updated_fields(document: Document) -> Document {
    crate::extjson_test_helper::flatten_extjson_document(document)
        .map(normalize_integers)
        .collect()
}

impl ChangeStreamEventDocument {
    fn assert_matches_expected(self, expected: ExpectedChangeStreamEventDocument, errmsg: &str) {
        assert_eq!(self.operation_type, expected.operation_type, "{}", errmsg);

        assert_eq!(self.ns, expected.ns, "{}", errmsg);

        assert_eq!(
            self.update_description
                .map(|desc| updated_fields(desc.updated_fields)),
            expected
                .update_description
                .map(|desc| updated_fields(desc.updated_fields)),
            "{}",
            errmsg
        );

        assert_eq!(
            full_document(self.full_document),
            full_document(expected.full_document),
            "{}",
            errmsg
        );
    }
}

fn run_test(
    test_case: TestCase,
    test_file: &TestFile,
    global_client: &Client,
) -> Result<Vec<ChangeStreamEventDocument>> {
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

    let client = Client::with_options(CHANGE_STREAM_CLIENT_OPTIONS.clone()).unwrap();

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
        operation.exec(&global_client)?;
    }

    match test_case.result {
        Outcome::Error { .. } => {
            change_stream.next().transpose()?;
            Ok(Vec::new())
        }
        Outcome::Success(_) => change_stream.collect(),
    }
}

fn run_change_stream_test(mut test_file: TestFile) {
    let global_client = Client::with_options(CHANGE_STREAM_CLIENT_OPTIONS.clone()).unwrap();

    let test_cases: Vec<_> = test_file.tests.drain(..).collect();

    for test_case in test_cases {
        let description = test_case.description.clone();

        let topology = match global_client.get_topology_type() {
            TopologyType::Single => Topology::Single,
            TopologyType::ReplicaSetNoPrimary | TopologyType::ReplicaSetWithPrimary => {
                Topology::ReplicaSet
            }
            TopologyType::Sharded => Topology::Sharded,
            TopologyType::Unknown => Topology::Unknown,
        };

        if !test_case.topology.contains(&topology) {
            continue;
        }

        if !crate::test::server_version_at_least(&global_client, &test_case.min_server_version) {
            continue;
        }

        match run_test(test_case.clone(), &test_file, &global_client) {
            Err(e) => match test_case.result {
                Outcome::Error { code } => match e.kind() {
                    ErrorKind::CommandError(ref inner) => {
                        assert_eq!(code, inner.code, "{}", description);
                    }
                    ErrorKind::ResponseError(_) => {
                        continue;
                    }
                    _ => panic!("{}: wrong type of error ({}) returned", &description, e),
                },
                Outcome::Success(_) => {
                    panic!("{}: unexpected error ({}) was returned", &description, e)
                }
            },
            Ok(changes) => match test_case.result {
                Outcome::Error { code } => panic!(
                    "{}: expected error (code: {}) was not returned",
                    &description, code
                ),
                Outcome::Success(docs) => {
                    assert_eq!(changes.len(), docs.len(), "{}", description);

                    for (actual, expected) in changes.into_iter().zip(docs) {
                        actual.assert_matches_expected(expected, &description);
                    }
                }
            },
        }

        // TODO RUST-200: Assert expectations == command monitoring results
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
