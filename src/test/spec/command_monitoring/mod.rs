mod event;
mod operation;

use std::convert::Into;

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Deserialize};

use self::{event::TestEvent, operation::*};
use crate::test::{assert_matches, parse_version, EventClient, CLIENT, LOCK};

#[derive(Deserialize)]
struct TestFile {
    data: Vec<Document>,
    collection_name: String,
    database_name: String,
    tests: Vec<TestCase>,
}

#[derive(Deserialize)]
struct TestCase {
    description: String,
    #[serde(rename = "ignore_if_server_version_greater_than", default)]
    max_version: Option<String>,
    #[serde(rename = "ignore_if_server_version_less_than", default)]
    min_version: Option<String>,
    operation: Document,
    expectations: Vec<TestEvent>,
}

fn run_command_monitoring_test<T: TestOperation + DeserializeOwned>(test_file_name: &str) {
    let test_file: TestFile = crate::test::spec::load_test(&["command-monitoring", test_file_name]);

    for test_case in test_file.tests {
        if let Some((major, minor)) = test_case.max_version.map(|s| parse_version(s.as_str())) {
            if CLIENT.server_version_gt(major, minor) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        if let Some((major, minor)) = test_case.min_version.map(|s| parse_version(s.as_str())) {
            if CLIENT.server_version_lt(major, minor) {
                println!("Skipping {}", test_case.description);
                continue;
            }
        }

        // We can't pass this test since it relies on old OP_QUERY behavior (SPEC-1519)
        if test_case.description.as_str()
            == "A successful find event with a getmore and the server kills the cursor"
        {
            continue;
        }

        let _guard = LOCK.run_exclusively();

        println!("Running {}", test_case.description);

        let operation: AnyTestOperation<T> =
            bson::from_bson(Bson::Document(test_case.operation)).unwrap();

        CLIENT
            .init_db_and_coll(&test_file.database_name, &test_file.collection_name)
            .insert_many(test_file.data.clone(), None)
            .expect("insert many error");

        let client = EventClient::new();

        let events: Vec<TestEvent> = client
            .run_operation_with_events(
                operation.command_names(),
                &test_file.database_name,
                &test_file.collection_name,
                |collection| {
                    let _ = operation.execute(collection);
                },
            )
            .into_iter()
            .map(Into::into)
            .collect();

        assert_eq!(events.len(), test_case.expectations.len());
        for (actual_event, expected_event) in events.iter().zip(test_case.expectations.iter()) {
            assert_matches(actual_event, expected_event);
        }
    }
}

#[test]
fn delete_many() {
    run_command_monitoring_test::<DeleteMany>("deleteMany.json")
}

#[test]
fn delete_one() {
    run_command_monitoring_test::<DeleteOne>("deleteOne.json")
}

#[test]
fn find() {
    run_command_monitoring_test::<Find>("find.json")
}

#[test]
fn insert_many() {
    run_command_monitoring_test::<InsertMany>("insertMany.json")
}

#[test]
fn insert_one() {
    run_command_monitoring_test::<InsertOne>("insertOne.json")
}

#[test]
fn update_many() {
    run_command_monitoring_test::<UpdateMany>("updateMany.json")
}

#[test]
fn update_one() {
    run_command_monitoring_test::<UpdateOne>("updateOne.json")
}
