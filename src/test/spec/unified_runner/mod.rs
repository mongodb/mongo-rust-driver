mod entity;
mod matchable;
mod operation;
mod test_file;

use std::collections::HashMap;

use lazy_static::lazy_static;
use semver::Version;

use crate::{
    bson::{doc, Bson},
    concern::{Acknowledgment, WriteConcern},
    options::{CollectionOptions, InsertManyOptions, ReadPreference, SelectionCriteria},
    test::{assert_matches, util::EventClient, TestClient},
};

pub use self::{
    entity::Entity,
    matchable::assert_results_match,
    operation::EntityOperation,
    test_file::{Entity as TestFileEntity, ErrorType, ExpectedError, Operation, TestFile},
};

lazy_static! {
    static ref SPEC_VERSIONS: Vec<Version> = vec![Version::parse("1.0.0").unwrap()];
}

#[allow(dead_code)]
pub async fn run_unified_format_test(test_file: TestFile) {
    let version_matches = SPEC_VERSIONS.iter().any(|req| {
        if req.major != test_file.schema_version.major {
            return false;
        }
        if req.minor < test_file.schema_version.minor {
            return false;
        }
        // patch versions do not affect the test file format
        true
    });
    if !version_matches {
        println!(
            "Test runner not compatible with specification version {}",
            &test_file.schema_version
        );
        return;
    }

    let client = TestClient::new().await;
    if let Some(run_on) = test_file.run_on {
        if !run_on.iter().any(|run_on| run_on.can_run_on(&client)) {
            println!("Client topology not compatible with test");
            return;
        }
    }

    for test_case in test_file.tests {
        if let Some(skip_reason) = test_case.skip_reason {
            println!("skipping {}: {}", &test_case.description, skip_reason);
            return;
        }

        if let Some(run_on) = test_case.run_on {
            if !run_on.iter().any(|run_on| run_on.can_run_on(&client)) {
                println!(
                    "{}: client topology not compatible with test",
                    &test_case.description
                );
                return;
            }
        }

        if let Some(ref initial_data) = test_file.initial_data {
            let write_concern = WriteConcern::builder().w(Acknowledgment::Majority).build();
            let collection_options = CollectionOptions::builder()
                .write_concern(write_concern.clone())
                .build();
            let coll = client
                .init_db_and_coll_with_options(
                    &initial_data.database_name,
                    &initial_data.collection_name,
                    collection_options,
                )
                .await;

            let insert_options = InsertManyOptions::builder()
                .write_concern(write_concern)
                .build();
            if !initial_data.documents.is_empty() {
                coll.insert_many(initial_data.documents.clone(), insert_options)
                    .await
                    .expect(&format!(
                        "{}: inserting initial data should succeed",
                        &test_case.description
                    ));
            }
        }

        let mut entities: HashMap<String, Entity> = HashMap::new();
        if let Some(ref create_entities) = test_file.create_entities {
            for entity in create_entities {
                match entity {
                    TestFileEntity::Client(client) => {
                        let id = client.id.clone();
                        let client = EventClient::unified_with_additional_options(
                            client.uri_options.clone(),
                            client.use_multiple_mongoses,
                        )
                        .await;
                        entities.insert(id, client.into());
                    }
                    TestFileEntity::Database(database) => {
                        let id = database.id.clone();
                        let client = entities.get(&database.client).unwrap().as_client();
                        let database = if let Some(ref options) = database.database_options {
                            let options = options.as_database_options();
                            client.database_with_options(&database.database_name, options)
                        } else {
                            client.database(&database.database_name)
                        };
                        entities.insert(id, database.into());
                    }
                    TestFileEntity::Collection(collection) => {
                        let id = collection.id.clone();
                        let database = entities.get(&collection.database).unwrap().as_database();
                        let collection = if let Some(ref options) = collection.collection_options {
                            let options = options.as_collection_options();
                            database.collection_with_options(&collection.collection_name, options)
                        } else {
                            database.collection(&collection.collection_name)
                        };
                        entities.insert(id, collection.into());
                    }
                    TestFileEntity::Session(_) => {
                        panic!(
                            "{}: Explicit sessions not implemented",
                            &test_case.description
                        );
                    }
                    TestFileEntity::Bucket(_) => {
                        panic!("{}: GridFS not implemented", &test_case.description);
                    }
                }
            }
        }

        for operation in test_case.operations {
            match operation.object.as_str() {
                "testRunner" => {
                    match operation.name.as_str() {
                        "failPoint" => {
                            // isabeltodo keep track of this failpoint to unset at end -- teardown
                            // struct?
                            let arguments = operation.arguments.unwrap();
                            let fail_point = arguments.get_document("failPoint").unwrap();
                            let client_id = arguments.get_str("client").unwrap();
                            let client = entities.get(client_id).unwrap().as_client();
                            let selection_criteria =
                                SelectionCriteria::ReadPreference(ReadPreference::Primary);
                            client
                                .database("admin")
                                .run_command(fail_point.clone(), selection_criteria)
                                .await
                                .unwrap();
                        }
                        "targetedFailPoint" => panic!("Transactions not implemented"),
                        "assertSessionTransactionState" => panic!("Transactions not implemented"),
                        "assertSessionPinned" => panic!("Transactions not implemented"),
                        "assertSessionUnpinned" => panic!("Transactions not implemented"),
                        "assertDifferentLsidOnLastTwoCommands" => {
                            let lsids = get_lsids(&operation, &entities);
                            assert_ne!(lsids.0, lsids.1);
                        }
                        "assertSameLsidOnLastTwoCommands" => {
                            let lsids = get_lsids(&operation, &entities);
                            assert_ne!(lsids.0, lsids.1);
                        }
                        "assertSessionDirty" => {
                            panic!("Explicit sessions not implemented");
                        }
                        "assertSessionNotDirty" => {
                            panic!("Explicit sessions not implemented");
                        }
                        "assertCollectionExists" => {
                            let arguments = operation.arguments.unwrap();
                            let database_name = arguments.get_str("databaseName").unwrap();
                            let collection_name = arguments.get_str("collectionName").unwrap();
                            let collections = client
                                .database(database_name)
                                .list_collection_names(doc! {})
                                .await
                                .unwrap();
                            assert!(collections.contains(&collection_name.to_string()));
                        }
                        "assertCollectionNotExists" => {
                            let arguments = operation.arguments.unwrap();
                            let database_name = arguments.get_str("databaseName").unwrap();
                            let collection_name = arguments.get_str("collectionName").unwrap();
                            let collections = client
                                .database(database_name)
                                .list_collection_names(doc! {})
                                .await
                                .unwrap();
                            assert!(!collections.contains(&collection_name.to_string()));
                        }
                        "assertIndexExists" => panic!("Index management not implemented"),
                        "assertIndexNotExists" => panic!("Index management not implemented"),
                        _ => {
                            let operation = EntityOperation::from_operation(operation).unwrap();
                            let result = if operation.object.starts_with("client") {
                                let client = entities.get(&operation.object).unwrap().as_client();
                                operation.execute_on_client(client).await
                            } else if operation.object.starts_with("database") {
                                let database =
                                    entities.get(&operation.object).unwrap().as_database();
                                operation.execute_on_database(database).await
                            } else if operation.object.starts_with("collection") {
                                let collection =
                                    entities.get(&operation.object).unwrap().as_collection();
                                operation.execute_on_collection(collection).await
                            } else {
                                panic!(
                                    "{}: {} not in entity map",
                                    &test_case.description, operation.name
                                );
                            };
                            if let Some(expected_result) = operation.expected_result {
                                let result =
                                    result.unwrap_or_else(|_| panic!("operation should succeed"));
                                match result {
                                    Some(result) => {
                                        assert_results_match(Some(&result), &expected_result)
                                    }
                                    None => {
                                        panic!("expected {}, got {:?}", &expected_result, &result);
                                    }
                                }
                            } else if let Some(expected_error) = operation.expected_error {
                                let error = result.unwrap_err();
                                if let Some(error_type) = expected_error.error_type {
                                    assert_eq!(
                                        error_type == ErrorType::Server,
                                        error.is_server_error()
                                    );
                                }
                                if let Some(error_contains) = expected_error.error_contains {
                                    match &error.kind.code_and_message() {
                                        Some((_, msg)) => assert!(msg.contains(&error_contains)),
                                        None => panic!("error should include message field"),
                                    }
                                }
                                if let Some(_error_code_name) = expected_error.error_code_name {
                                    // TODO parse error code names
                                }
                                if let Some(error_labels_contain) =
                                    expected_error.error_labels_contain
                                {
                                    for label in error_labels_contain {
                                        assert!(error.labels().contains(&label));
                                    }
                                }
                                if let Some(error_labels_omit) = expected_error.error_labels_omit {
                                    for label in error_labels_omit {
                                        assert!(!error.labels().contains(&label));
                                    }
                                }
                                if expected_error.expected_result.is_some() {
                                    panic!("Bulk write not implemented");
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(expect_events) = test_case.expect_events {
            for expected in expect_events {
                let mut expected_events = expected.events.iter();
                let client = entities.get(&expected.client).unwrap().as_client();
                let actual_events = client.get_test_events();
                for event in actual_events {
                    // TODO check to see if the event should be ignored based on its name
                    let expected_event = expected_events.next().unwrap();
                    assert_matches(&event, expected_event, None);
                }
            }
        }
    }
}

fn get_lsids(operation: &Operation, entities: &HashMap<String, Entity>) -> (Bson, Bson) {
    let arguments = operation.arguments.as_ref().unwrap();
    let client_id = arguments.get_str("client").unwrap();
    let client = entities.get(client_id).unwrap().as_client();
    let events = client.get_all_command_started_events();
    assert!(events.len() >= 2);
    let last = events.last().unwrap();
    let second_last = events.get(events.len() - 2).unwrap();
    let lsid1 = last.command.get("lsid").unwrap().clone();
    let lsid2 = second_last.command.get("lsid").unwrap().clone();
    (lsid1, lsid2)
}
