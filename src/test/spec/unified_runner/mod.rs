mod operation;
mod test_file;

use std::collections::HashMap;

use lazy_static::lazy_static;
use semver::Version;

use crate::{
    bson::{doc, spec::ElementType, Bson},
    concern::{Acknowledgment, WriteConcern},
    options::{CollectionOptions, InsertManyOptions, ReadPreference, SelectionCriteria},
    test::{util::EventClient, TestClient},
    Collection,
    Database,
};

pub use self::{
    operation::EntityOperation,
    test_file::{Entity, ErrorType, ExpectedError, Operation, TestFile},
};

lazy_static! {
    static ref SPEC_VERSION: Version = Version::parse("1.0.0").unwrap();
}

struct EntityMap {
    pub clients: HashMap<String, EventClient>,
    pub databases: HashMap<String, Database>,
    pub collections: HashMap<String, Collection>,
}

impl EntityMap {
    fn new() -> Self {
        EntityMap {
            clients: HashMap::new(),
            databases: HashMap::new(),
            collections: HashMap::new(),
        }
    }

    fn add_client(&mut self, id: String, client: EventClient) {
        self.clients.insert(id, client);
    }

    fn get_client(&self, id: &str) -> &EventClient {
        self.clients.get(id).expect("")
    }

    fn add_database(&mut self, id: String, database: Database) {
        self.databases.insert(id, database);
    }

    fn get_database(&self, id: &str) -> &Database {
        self.databases.get(id).expect("")
    }

    fn add_collection(&mut self, id: String, collection: Collection) {
        self.collections.insert(id, collection);
    }

    fn get_collection(&self, id: &str) -> &Collection {
        self.collections.get(id).expect("")
    }
}

#[allow(dead_code)]
pub async fn run_unified_format_test(test_file: TestFile) {
    if !test_file.schema_version.matches(&SPEC_VERSION) {
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

        let mut entities = EntityMap::new();
        if let Some(ref create_entities) = test_file.create_entities {
            for entity in create_entities {
                match entity {
                    Entity::Client(client) => {
                        let id = client.id.clone();
                        let client = EventClient::with_additional_options(
                            client.uri_options.clone(),
                            None,
                            // isabeltodo update use_multiple_mongoses to fit spec within
                            // with_additional_options
                            client.use_multiple_mongoses,
                        )
                        .await;
                        entities.add_client(id, client);
                    }
                    Entity::Database(database) => {
                        let id = database.id.clone();
                        let client = entities.get_client(&database.client);
                        let database = if let Some(ref options) = database.database_options {
                            let options = options.as_database_options();
                            client.database_with_options(&database.database_name, options)
                        } else {
                            client.database(&database.database_name)
                        };
                        entities.add_database(id, database);
                    }
                    Entity::Collection(collection) => {
                        let id = collection.id.clone();
                        let database = entities.get_database(&collection.database);
                        let collection = if let Some(ref options) = collection.collection_options {
                            let options = options.as_collection_options();
                            database.collection_with_options(&collection.collection_name, options)
                        } else {
                            database.collection(&collection.collection_name)
                        };
                        entities.add_collection(id, collection);
                    }
                    Entity::Session(_) => {
                        panic!(
                            "{}: explicit sessions not implemented",
                            &test_case.description
                        );
                    }
                    Entity::Bucket(_) => {
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
                            let client = entities.get_client(client_id);
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
                            let arguments = operation.arguments.unwrap();
                            let client_id = arguments.get_str("client").unwrap();
                            let client = entities.get_client(client_id);
                            let events = client.get_all_command_started_events();
                            assert!(events.len() >= 2);
                            let last = events.last().unwrap();
                            let second_last = events.get(events.len() - 2).unwrap();
                            let lsid1 = last.command.get("lsid").unwrap();
                            let lsid2 = second_last.command.get("lsid").unwrap();
                            assert_ne!(lsid1, lsid2);
                        }
                        "assertSameLsidOnLastTwoCommands" => {
                            // isabeltodo make method for this
                            let arguments = operation.arguments.unwrap();
                            let client_id = arguments.get_str("client").unwrap();
                            let client = entities.get_client(client_id);
                            let events = client.get_all_command_started_events();
                            assert!(events.len() >= 2);
                            let last = events.last().unwrap();
                            let second_last = events.get(events.len() - 2).unwrap();
                            let lsid1 = last.command.get("lsid").unwrap();
                            let lsid2 = second_last.command.get("lsid").unwrap();
                            assert_eq!(lsid1, lsid2);
                        }
                        "assertSessionDirty" => {
                            // isabeltodo add sessions to entity map
                        }
                        "assertSessionNotDirty" => {
                            // isabeltodo add sessions to entity map
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
                                let client = entities.get_client(&operation.object);
                                operation.execute_on_client(client).await
                            } else if operation.object.starts_with("database") {
                                let database = entities.get_database(&operation.object);
                                operation.execute_on_database(database).await
                            } else if operation.object.starts_with("collection") {
                                let collection = entities.get_collection(&operation.object);
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
                                    // isabeltodo figure out how to get error code names
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
                                if let Some(_expected_result) = expected_error.expected_result {
                                    panic!("bulk write not implemented");
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

fn assert_results_match(actual: Option<&Bson>, expected: &Bson) {
    match expected {
        Bson::Document(expected_doc) => {
            if let Some(special_op) = expected_doc.iter().next() {
                if special_op.0.starts_with("$$") {
                    assert_special_operator_matches(special_op, actual);
                }
            }
            let actual_doc = actual.unwrap().as_document().unwrap();
            for (key, value) in expected_doc {
                assert_results_match(actual_doc.get(key), value);
            }
        }
        Bson::Array(expected_array) => {
            let actual_array = actual.unwrap().as_array().unwrap();
            assert_eq!(expected_array.len(), actual_array.len());
            for i in 0..expected_array.len() {
                assert_results_match(Some(&actual_array[i]), &expected_array[i]);
            }
        }
        _ => assert_eq!(actual.unwrap(), expected),
    }
}

fn assert_special_operator_matches(special_op: (&String, &Bson), actual: Option<&Bson>) {
    match special_op.0.as_ref() {
        "$$exists" => {
            assert_eq!(special_op.1.as_bool().unwrap(), actual.is_some());
        }
        "$$type" => {
            assert!(type_matches(special_op.1, actual.unwrap()));
        }
        "$$unsetOrMatches" => if let Some(bson) = actual {
            assert_results_match(Some(special_op.1), bson);
        }
        "$$sessionLsid" => {
            // isabeltodo figure out how to do this with the entity map
        }
        other => panic!("unknown special operator: {}", other),
    }
}

fn type_matches(types: &Bson, actual: &Bson) -> bool {
    match types {
        Bson::Array(types) => {
            types.iter().any(|t| type_matches(t, actual))
        }
        Bson::String(str) => match str.as_ref() {
            "double" => actual.element_type() == ElementType::Double,
            "string" => actual.element_type() == ElementType::String,
            "object" => actual.element_type() == ElementType::EmbeddedDocument,
            "array" => actual.element_type() == ElementType::Array,
            "binData" => actual.element_type() == ElementType::Binary,
            "undefined" => actual.element_type() == ElementType::Undefined,
            "objectId" => actual.element_type() == ElementType::ObjectId,
            "bool" => actual.element_type() == ElementType::Boolean,
            "date" => actual.element_type() == ElementType::DateTime,
            "null" => actual.element_type() == ElementType::Null,
            "regex" => actual.element_type() == ElementType::RegularExpression,
            "dbPointer" => actual.element_type() == ElementType::DbPointer,
            "javascript" => actual.element_type() == ElementType::JavaScriptCode,
            "symbol" => actual.element_type() == ElementType::Symbol,
            "javascriptWithScope" => {
                actual.element_type() == ElementType::JavaScriptCodeWithScope
            }
            "int" => actual.element_type() == ElementType::Int32,
            "timestamp" => actual.element_type() == ElementType::Timestamp,
            "long" => actual.element_type() == ElementType::Int64,
            "decimal" => actual.element_type() == ElementType::Decimal128,
            "minKey" => actual.element_type() == ElementType::MinKey,
            "maxKey" => actual.element_type() == ElementType::MaxKey,
            other => panic!("unrecognized type: {}", other),
        },
        // isabeltodo verify that the number gets serialized into an i32
        Bson::Int32(n) => match n {
            1 => actual.element_type() == ElementType::Double,
            2 => actual.element_type() == ElementType::String,
            3 => actual.element_type() == ElementType::EmbeddedDocument,
            4 => actual.element_type() == ElementType::Array,
            5 => actual.element_type() == ElementType::Binary,
            6 => actual.element_type() == ElementType::Undefined,
            7 => actual.element_type() == ElementType::ObjectId,
            8 => actual.element_type() == ElementType::Boolean,
            9 => actual.element_type() == ElementType::DateTime,
            10 => actual.element_type() == ElementType::Null,
            11 => actual.element_type() == ElementType::RegularExpression,
            12 => actual.element_type() == ElementType::DbPointer,
            13 => actual.element_type() == ElementType::JavaScriptCode,
            14 => actual.element_type() == ElementType::Symbol,
            15 => actual.element_type() == ElementType::JavaScriptCodeWithScope,
            16 => actual.element_type() == ElementType::Int32,
            17 => actual.element_type() == ElementType::Timestamp,
            18 => actual.element_type() == ElementType::Int64,
            19 => actual.element_type() == ElementType::Decimal128,
            -1 => actual.element_type() == ElementType::MinKey,
            127 => actual.element_type() == ElementType::MaxKey,
            other => panic!("unrecognized type: {}", other),
        },
        other => panic!("unrecognized type: {}", other),
    }
}
