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
    options::{CollectionOptions, FindOptions, ReadConcern, ReadPreference, SelectionCriteria},
    test::{assert_matches, run_spec_test, util::EventClient, TestClient, LOCK},
};

pub use self::{
    entity::{ClientEntity, Entity},
    matcher::results_match,
    operation::{Operation, OperationObject},
    test_event::TestEvent,
    test_file::{CollectionData, Entity as TestFileEntity, ExpectError, TestFile, Topology},
};

lazy_static! {
    static ref SPEC_VERSIONS: Vec<Version> = vec![Version::parse("1.0.0").unwrap()];
}

pub struct TestRunner {
    pub internal_client: TestClient,
    pub entities: HashMap<String, Entity>,
    pub failpoint_disable_commands: Vec<Document>,
}

impl TestRunner {
    pub async fn new() -> Self {
        Self {
            internal_client: TestClient::new().await,
            entities: HashMap::new(),
            failpoint_disable_commands: Vec::new(),
        }
    }

    pub async fn insert_initial_data(&self, data: &CollectionData) {
        let write_concern = WriteConcern::builder().w(Acknowledgment::Majority).build();
        let collection_options = CollectionOptions::builder()
            .write_concern(write_concern)
            .build();
        let coll = self
            .internal_client
            .init_db_and_coll_with_options(
                &data.database_name,
                &data.collection_name,
                collection_options,
            )
            .await;

        if !data.documents.is_empty() {
            coll.insert_many(data.documents.clone(), None)
                .await
                .unwrap();
        }
    }

    pub async fn disable_fail_points(&self) {
        for disable_command in &self.failpoint_disable_commands {
            self.internal_client
                .database("admin")
                .run_command(disable_command.clone(), None)
                .await
                .unwrap();
        }
    }

    pub async fn populate_entity_map(&mut self, create_entities: &[TestFileEntity]) {
        self.entities.clear();

        for entity in create_entities {
            match entity {
                TestFileEntity::Client(client) => {
                    let id = client.id.clone();
                    let observe_events = client.observe_events.clone();
                    let ignore_command_names = client.ignore_command_monitoring_events.clone();
                    let client = EventClient::with_uri_and_mongos_options(
                        &client.uri,
                        client.use_multiple_mongoses,
                    )
                    .await;
                    self.entities.insert(
                        id,
                        Entity::from_client(client, observe_events, ignore_command_names),
                    );
                }
                TestFileEntity::Database(database) => {
                    let id = database.id.clone();
                    let client = self.entities.get(&database.client).unwrap().as_client();
                    let database = if let Some(ref options) = database.database_options {
                        let options = options.as_database_options();
                        client.database_with_options(&database.database_name, options)
                    } else {
                        client.database(&database.database_name)
                    };
                    self.entities.insert(id, database.into());
                }
                TestFileEntity::Collection(collection) => {
                    let id = collection.id.clone();
                    let database = self
                        .entities
                        .get(&collection.database)
                        .unwrap()
                        .as_database();
                    let collection = if let Some(ref options) = collection.collection_options {
                        let options = options.as_collection_options();
                        database.collection_with_options(&collection.collection_name, options)
                    } else {
                        database.collection(&collection.collection_name)
                    };
                    self.entities.insert(id, collection.into());
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

    let mut test_runner = TestRunner::new().await;

    if let Some(requirements) = test_file.run_on_requirements {
        if !requirements
            .iter()
            .any(|requirement| requirement.can_run_on(&test_runner.internal_client))
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
                .any(|requirement| requirement.can_run_on(&test_runner.internal_client))
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
                test_runner.insert_initial_data(data).await;
            }
        }

        if let Some(ref create_entities) = test_file.create_entities {
            test_runner.populate_entity_map(create_entities).await;
        }

        for operation in test_case.operations {
            let result = operation.execute(&operation.object, &mut test_runner).await;

            if let Some(id) = operation.save_result_as_entity {
                match &result {
                    Ok(Some(entity)) => {
                        test_runner.entities.insert(id, entity.clone());
                    }
                    Ok(None) => panic!("{} did not return an entity", operation.name),
                    Err(_) => panic!("operation should succeed"),
                }
            }

            if let Some(expect_result) = operation.expect_result {
                let result = result
                    .unwrap_or_else(|_| panic!("operation should succeed"))
                    .unwrap_or_else(|| panic!("operation should return an entity"));
                match result {
                    Entity::Bson(ref result) => {
                        assert!(results_match(Some(result), &expect_result));
                    }
                    _ => panic!("incorrect entity type returned"),
                }
            } else if let Some(expect_error) = operation.expect_error {
                let error = result.expect_err("operation should return error");
                expect_error.verify_result(error);
            }
        }

        test_runner.disable_fail_points().await;

        if let Some(ref events) = test_case.expect_events {
            for expected in events {
                let entity = test_runner.entities.get(&expected.client).unwrap();
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
                let collection = test_runner
                    .internal_client
                    .get_coll_with_options(db_name, coll_name, options);

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

#[cfg_attr(feature = "tokio-runtime", tokio::test)]
#[cfg_attr(feature = "async-std-runtime", async_std::test)]
async fn test_examples() {
    let _guard = LOCK.run_exclusively().await;
    run_spec_test(&["unified-runner-examples"], run_unified_format_test).await;
}
