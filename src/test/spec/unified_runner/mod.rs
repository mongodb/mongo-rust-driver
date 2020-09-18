mod entity;
mod matcher;
mod operation;
mod test_event;
mod test_file;

use std::collections::HashMap;

use futures::stream::TryStreamExt;
use lazy_static::lazy_static;
use semver::Version;

use crate::{
    bson::{doc, Document},
    concern::{Acknowledgment, WriteConcern},
    options::{
        CollectionOptions,
        FindOptions,
        InsertManyOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
    },
    test::{assert_matches, run_spec_test, util::EventClient, TestClient, LOCK},
};

pub use self::{
    entity::Entity,
    matcher::results_match,
    operation::EntityOperation,
    test_event::TestEvent,
    test_file::{
        CollectionData,
        Entity as TestFileEntity,
        ExpectError,
        Operation,
        TestFile,
        Topology,
    },
};

lazy_static! {
    static ref SPEC_VERSIONS: Vec<Version> = vec![Version::parse("1.0.0").unwrap()];
}

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
    if let Some(requirements) = test_file.run_on_requirements {
        if !requirements
            .iter()
            .any(|requirement| requirement.can_run_on(&client))
        {
            println!("Client topology not compatible with test");
            return;
        }
    }

    for test_case in test_file.tests {
        if let Some(skip_reason) = test_case.skip_reason {
            println!("skipping {}: {}", &test_case.description, skip_reason);
            return;
        }

        if let Some(requirements) = test_case.run_on_requirements {
            if !requirements
                .iter()
                .any(|requirement| requirement.can_run_on(&client))
            {
                println!(
                    "{}: client topology not compatible with test",
                    &test_case.description
                );
                return;
            }
        }

        if let Some(ref initial_data) = test_file.initial_data {
            for data in initial_data {
                insert_initial_data(data, &client).await;
            }
        }

        let mut entities: HashMap<String, Entity> = HashMap::new();
        if let Some(ref create_entities) = test_file.create_entities {
            populate_entity_map(&mut entities, create_entities).await;
        }

        let mut failpoint_disable_commands: Vec<Document> = Vec::new();
        for operation in test_case.operations {
            if operation.object.as_str() == "testRunner" {
                match operation.name.as_str() {
                    "failPoint" => {
                        let arguments = operation.arguments.unwrap();
                        let fail_point = arguments.get_document("failPoint").unwrap();

                        let disable_command = doc! {
                            "configureFailPoint": fail_point.get_str("configureFailPoint").unwrap(),
                            "mode": "off",
                        };
                        failpoint_disable_commands.push(disable_command);

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
                    "targetedFailPoint"
                    | "assertSessionTransactionState"
                    | "assertSessionPinned"
                    | "assertSessionUnpinned" => panic!("Transactions not implemented"),
                    "assertDifferentLsidOnLastTwoCommands"
                    | "assertSameLsidOnLastTwoCommands"
                    | "assertSessionNotDirty" => panic!("Explicit sessions not implemented"),
                    "assertCollectionExists" => {
                        let arguments = operation.arguments.unwrap();
                        let collection_name = arguments.get_str("collectionName").unwrap();
                        assert!(get_collection_names(&arguments, &client)
                            .await
                            .contains(&collection_name.to_string()));
                    }
                    "assertCollectionNotExists" => {
                        let arguments = operation.arguments.unwrap();
                        let collection_name = arguments.get_str("collectionName").unwrap();
                        assert!(!get_collection_names(&arguments, &client)
                            .await
                            .contains(&collection_name.to_string()));
                    }
                    "assertIndexExists" | "assertIndexNotExists" => {
                        panic!("Index management not implemented")
                    }
                    other => panic!("Unknown test runner operation: {}", other),
                }
            } else {
                let operation = EntityOperation::from_operation(operation).unwrap();

                let result = operation.execute(&entities).await;

                if let Some(id) = operation.save_result_as_entity {
                    let result = result.clone().unwrap();
                    entities.insert(id, result.into());
                }

                if let Some(expect_result) = operation.expect_result {
                    let result = result.unwrap_or_else(|_| panic!("operation should succeed"));
                    match result {
                        Some(result) => {
                            assert!(results_match(Some(&result), &expect_result));
                        }
                        None => {
                            panic!("expected {}, got {:?}", &expect_result, &result);
                        }
                    }
                } else if let Some(expect_error) = operation.expect_error {
                    let error = result.unwrap_err();
                    expect_error.verify_result(error);
                }
            }
        }

        if let Some(ref events) = test_case.expect_events {
            for expected in events {
                let entity = entities.get(&expected.client).unwrap();
                let client = entity.as_client();

                let actual_events: Vec<TestEvent> = client
                    .get_filtered_events(&client.observe_events, &client.ignore_command_names)
                    .into_iter()
                    .map(Into::into)
                    .collect();

                let expected_events = &expected.events;

                assert_eq!(actual_events.len(), expected_events.len());

                for (actual, expected) in actual_events.iter().zip(expected_events) {
                    assert_matches(actual, expected, None);
                }
            }
        }

        // Disable any fail points that were set when running the operations.
        for disable_command in failpoint_disable_commands {
            client
                .database("admin")
                .run_command(disable_command, None)
                .await
                .unwrap();
        }

        if let Some(ref outcome) = test_case.outcome {
            for expected_data in outcome {
                let db_name = &expected_data.database_name;
                let coll_name = &expected_data.collection_name;

                let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
                let read_concern = ReadConcern::local();

                let options = CollectionOptions::builder()
                    .selection_criteria(selection_criteria)
                    .read_concern(read_concern)
                    .build();
                let collection = client.get_coll_with_options(db_name, coll_name, options);

                let options = FindOptions::builder().sort(doc! { "_id": 1 }).build();
                let actual_data: Vec<Document> = collection
                    .find(doc! {}, options)
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();

                assert_eq!(expected_data.documents, actual_data);
            }
        }
    }
}

async fn populate_entity_map(
    entities: &mut HashMap<String, Entity>,
    create_entities: &[TestFileEntity],
) {
    for entity in create_entities {
        match entity {
            TestFileEntity::Client(client) => {
                let id = client.id.clone();
                let observe_events = client.observe_events.clone();
                let ignore_command_names = client.ignore_command_monitoring_events.clone();
                let client = EventClient::with_additional_and_mongos_options(
                    client.uri_options.clone(),
                    client.use_multiple_mongoses,
                )
                .await;
                entities.insert(
                    id,
                    Entity::from_client(client, observe_events, ignore_command_names),
                );
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
                panic!("Explicit sessions not implemented");
            }
            TestFileEntity::Bucket(_) | TestFileEntity::Stream(_) => {
                panic!("GridFS not implemented");
            }
        }
    }
}

async fn insert_initial_data(data: &CollectionData, client: &TestClient) {
    let write_concern = WriteConcern::builder().w(Acknowledgment::Majority).build();
    let collection_options = CollectionOptions::builder()
        .write_concern(write_concern.clone())
        .build();
    let coll = client
        .init_db_and_coll_with_options(
            &data.database_name,
            &data.collection_name,
            collection_options,
        )
        .await;

    let insert_options = InsertManyOptions::builder()
        .write_concern(write_concern)
        .build();
    if !data.documents.is_empty() {
        coll.insert_many(data.documents.clone(), insert_options)
            .await
            .unwrap();
    }
}

async fn get_collection_names(arguments: &Document, client: &TestClient) -> Vec<String> {
    let database_name = arguments.get_str("databaseName").unwrap();
    let collection_name = arguments.get_str("collectionName").unwrap();
    client
        .database(database_name)
        .list_collection_names(doc! { "name": collection_name })
        .await
        .unwrap()
}

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_examples() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["unified-runner-examples"], run_unified_format_test).await;
}
