use std::{collections::HashMap, fs::File, io::BufWriter, sync::Arc, time::Duration};

use futures::TryStreamExt;

use crate::{
    bson::{doc, Document},
    client::options::ClientOptions,
    concern::{Acknowledgment, WriteConcern},
    options::{
        CollectionOptions,
        CreateCollectionOptions,
        FindOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
    },
    runtime,
    test::{
        log_uncaptured,
        spec::unified_runner::{
            entity::EventList,
            matcher::{events_match, results_match},
            operation::{Expectation, OperationObject},
            test_file::{ExpectedEventType, TestCase, TestFile},
        },
        update_options_for_testing,
        util::FailPointGuard,
        EventHandler,
        TestClient,
        CLIENT_OPTIONS,
        DEFAULT_URI,
        LOAD_BALANCED_MULTIPLE_URI,
        LOAD_BALANCED_SINGLE_URI,
        SERVERLESS,
        SERVER_API,
    },
    Client,
    Collection,
    Database,
};

use super::{
    merge_uri_options,
    ClientEntity,
    CollectionData,
    Entity,
    SessionEntity,
    TestCursor,
    TestFileEntity,
};

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
    "count",
    "download",
    "download_by_name",
    "listCollectionObjects",
    "listDatabaseObjects",
    "mapReduce",
    "watch",
];

pub type EntityMap = HashMap<String, Entity>;

pub struct TestRunner {
    pub internal_client: TestClient,
    pub entities: EntityMap,
    pub fail_point_guards: Vec<FailPointGuard>,
}

impl TestRunner {
    pub async fn new() -> Self {
        Self {
            internal_client: TestClient::new().await,
            entities: HashMap::new(),
            fail_point_guards: Vec::new(),
        }
    }

    pub async fn new_with_connection_string(connection_string: &str) -> Self {
        #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
        let options = ClientOptions::parse(connection_string).await.unwrap();
        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        let options = ClientOptions::parse(connection_string).unwrap();
        Self {
            internal_client: TestClient::with_options(Some(options)).await,
            entities: HashMap::new(),
            fail_point_guards: Vec::new(),
        }
    }

    pub async fn run_test(&mut self, test_file: TestFile, pred: impl Fn(&TestCase) -> bool) {
        if let Some(requirements) = test_file.run_on_requirements {
            let mut can_run_on = false;
            for requirement in requirements {
                if requirement.can_run_on(&self.internal_client).await {
                    can_run_on = true;
                }
            }
            if !can_run_on {
                log_uncaptured("Client topology not compatible with test");
                return;
            }
        }

        for test_case in test_file.tests {
            if let Some(skip_reason) = test_case.skip_reason {
                log_uncaptured(format!(
                    "Skipping {}: {}",
                    &test_case.description, skip_reason
                ));
                continue;
            }

            if let Some(op) = test_case
                .operations
                .iter()
                .find(|op| SKIPPED_OPERATIONS.contains(&op.name.as_str()))
                .map(|op| op.name.as_str())
            {
                log_uncaptured(format!(
                    "Skipping {}: unsupported operation {}",
                    &test_case.description, op
                ));
                continue;
            }

            if !pred(&test_case) {
                log_uncaptured(format!(
                    "Skipping {}: predicate failed",
                    test_case.description
                ));
                continue;
            }

            log_uncaptured(format!("Running {}", &test_case.description));

            if let Some(requirements) = test_case.run_on_requirements {
                let mut can_run_on = false;
                for requirement in requirements {
                    if requirement.can_run_on(&self.internal_client).await {
                        can_run_on = true;
                    }
                }
                if !can_run_on {
                    log_uncaptured(format!(
                        "{}: client topology not compatible with test",
                        &test_case.description
                    ));
                    continue;
                }
            }

            if let Some(ref initial_data) = test_file.initial_data {
                for data in initial_data {
                    self.insert_initial_data(data).await;
                }
            }

            if let Some(ref create_entities) = test_file.create_entities {
                self.populate_entity_map(create_entities).await;
            }

            for operation in test_case.operations {
                self.sync_workers().await;
                match operation.object {
                    OperationObject::TestRunner => {
                        operation.execute_test_runner_operation(self).await;
                    }
                    OperationObject::Entity(ref id) => {
                        let result = operation.execute_entity_operation(id, self).await;

                        match &operation.expectation {
                            Expectation::Result {
                                expected_value,
                                save_as_entity,
                            } => {
                                let desc = &test_case.description;
                                let opt_entity = result.unwrap_or_else(|e| {
                                    panic!(
                                        "[{}] {} should succeed, but failed with the following \
                                         error: {}",
                                        desc, operation.name, e
                                    )
                                });
                                if expected_value.is_some() || save_as_entity.is_some() {
                                    let entity = opt_entity.unwrap_or_else(|| {
                                        panic!(
                                            "[{}] {} did not return an entity",
                                            desc, operation.name
                                        )
                                    });
                                    if let Some(expected_bson) = expected_value {
                                        if let Entity::Bson(actual) = &entity {
                                            if let Err(e) = results_match(
                                                Some(actual),
                                                expected_bson,
                                                operation.returns_root_documents(),
                                                Some(&self.entities),
                                            ) {
                                                panic!(
                                                    "[{}] result mismatch, expected = {:#?}  \
                                                     actual = {:#?}\nmismatch detail: {}",
                                                    desc, expected_bson, actual, e
                                                );
                                            }
                                        } else {
                                            panic!(
                                                "[{}] Incorrect entity type returned from {}, \
                                                 expected BSON",
                                                desc, operation.name
                                            );
                                        }
                                    }
                                    if let Some(id) = save_as_entity {
                                        self.insert_entity(id, entity);
                                    }
                                }
                            }
                            Expectation::Error(expect_error) => {
                                let error = result.expect_err(&format!(
                                    "{}: {} should return an error",
                                    test_case.description, operation.name
                                ));
                                expect_error.verify_result(&error);
                            }
                            Expectation::Ignore => (),
                        }
                    }
                }
                // This test (in src/test/spec/json/sessions/server-support.json) runs two
                // operations with implicit sessions in sequence and then checks to see if they
                // used the same lsid. We delay for one second to ensure that the
                // implicit session used in the first operation is returned to the pool before
                // the second operation is executed.
                if test_case.description == "Server supports implicit sessions" {
                    runtime::delay_for(Duration::from_secs(1)).await;
                }
            }

            if let Some(ref events) = test_case.expect_events {
                for expected in events {
                    let entity = self.entities.get(&expected.client).unwrap();
                    let client = entity.as_client();
                    client.sync_workers().await;
                    let event_type = expected.event_type.unwrap_or(ExpectedEventType::Command);

                    let actual_events: Vec<_> =
                        client.get_filtered_events(event_type).into_iter().collect();

                    let expected_events = &expected.events;

                    if expected.ignore_extra_events.unwrap_or(false) {
                        assert!(
                            actual_events.len() >= expected_events.len(),
                            "actual:\n{:#?}\nexpected:\n{:#?}",
                            actual_events,
                            expected_events
                        )
                    } else {
                        assert_eq!(
                            actual_events.len(),
                            expected_events.len(),
                            "actual:\n{:#?}\nexpected:\n{:#?}",
                            actual_events,
                            expected_events
                        )
                    }

                    for (actual, expected) in actual_events.iter().zip(expected_events) {
                        if let Err(e) = events_match(actual, expected, Some(&self.entities)) {
                            panic!(
                                "event mismatch: expected = {:#?}, actual = {:#?}\nall \
                                 expected:\n{:#?}\nall actual:\n{:#?}\nmismatch detail: {}",
                                expected, actual, expected_events, actual_events, e,
                            );
                        }
                    }
                }
            }

            self.fail_point_guards.clear();

            if let Some(ref outcome) = test_case.outcome {
                for expected_data in outcome {
                    let db_name = &expected_data.database_name;
                    let coll_name = &expected_data.collection_name;

                    let selection_criteria =
                        SelectionCriteria::ReadPreference(ReadPreference::Primary);
                    let read_concern = ReadConcern::local();

                    let options = CollectionOptions::builder()
                        .selection_criteria(selection_criteria)
                        .read_concern(read_concern)
                        .build();
                    let collection = self
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

            println!("{} succeeded", &test_case.description);
        }
    }

    pub async fn insert_initial_data(&self, data: &CollectionData) {
        let write_concern = WriteConcern::builder().w(Acknowledgment::Majority).build();

        if !data.documents.is_empty() {
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
            coll.insert_many(data.documents.clone(), None)
                .await
                .unwrap();
        } else {
            let collection_options = CreateCollectionOptions::builder()
                .write_concern(write_concern)
                .build();
            self.internal_client
                .create_fresh_collection(
                    &data.database_name,
                    &data.collection_name,
                    collection_options,
                )
                .await;
        }
    }

    pub async fn populate_entity_map(&mut self, create_entities: &[TestFileEntity]) {
        self.entities.clear();

        for entity in create_entities {
            let (id, entity) = match entity {
                TestFileEntity::Client(client) => {
                    if let Some(store_events_as_entities) = &client.store_events_as_entities {
                        for store_events_as_entity in store_events_as_entities {
                            let event_list = EventList {
                                client_id: client.id.clone(),
                                event_names: store_events_as_entity.events.clone(),
                            };
                            self.insert_entity(&store_events_as_entity.id, event_list.into());
                        }
                    }

                    let id = client.id.clone();
                    let observe_events = client.observe_events.clone();
                    let ignore_command_names = client.ignore_command_monitoring_events.clone();
                    let observe_sensitive_commands =
                        client.observe_sensitive_commands.unwrap_or(false);
                    let server_api = client.server_api.clone().or_else(|| SERVER_API.clone());
                    let observer = Arc::new(EventHandler::new());

                    let given_uri = if CLIENT_OPTIONS.get().await.load_balanced.unwrap_or(false) {
                        // for serverless testing, ignore use_multiple_mongoses.
                        if client.use_multiple_mongoses.unwrap_or(true) && !*SERVERLESS {
                            LOAD_BALANCED_MULTIPLE_URI.as_ref().expect(
                                "Test requires URI for load balancer fronting multiple servers",
                            )
                        } else {
                            LOAD_BALANCED_SINGLE_URI.as_ref().expect(
                                "Test requires URI for load balancer fronting single server",
                            )
                        }
                    } else {
                        &DEFAULT_URI
                    };
                    let uri = merge_uri_options(given_uri, client.uri_options.as_ref());
                    let mut options = ClientOptions::parse_uri(&uri, None).await.unwrap();
                    update_options_for_testing(&mut options);
                    options.command_event_handler = Some(observer.clone());
                    options.cmap_event_handler = Some(observer.clone());
                    options.sdam_event_handler = Some(observer.clone());
                    options.server_api = server_api;

                    if let Some(use_multiple_mongoses) = client.use_multiple_mongoses {
                        if TestClient::new().await.is_sharded() {
                            if use_multiple_mongoses {
                                assert!(
                                    options.hosts.len() > 1,
                                    "Test requires multiple mongos hosts"
                                );
                            } else {
                                options.hosts.drain(1..);
                            }
                        }
                    }

                    let client = Client::with_options(options).unwrap();

                    (
                        id,
                        Entity::Client(ClientEntity::new(
                            client,
                            observer,
                            observe_events,
                            ignore_command_names,
                            observe_sensitive_commands,
                        )),
                    )
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
                    (id, database.into())
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
                    (id, collection.into())
                }
                TestFileEntity::Session(session) => {
                    let id = session.id.clone();
                    let client = self.get_client(&session.client);
                    let client_session = client
                        .start_session(session.session_options.clone())
                        .await
                        .unwrap();
                    (id, Entity::Session(SessionEntity::new(client_session)))
                }
                TestFileEntity::Bucket(_) => {
                    panic!("GridFS not implemented");
                }
            };
            self.insert_entity(&id, entity);
        }
    }

    pub fn insert_entity(&mut self, id: &str, entity: Entity) {
        if self.entities.insert(id.to_string(), entity).is_some() {
            panic!("Entity with id {} already present in entity map", id);
        }
    }

    pub async fn sync_workers(&self) {
        self.internal_client.sync_workers().await;
        for entity in self.entities.values() {
            if let Entity::Client(client) = entity {
                client.sync_workers().await;
            }
        }
    }

    pub fn get_client(&self, id: &str) -> &ClientEntity {
        self.entities.get(id).unwrap().as_client()
    }

    pub fn get_database(&self, id: &str) -> &Database {
        self.entities.get(id).unwrap().as_database()
    }

    pub fn get_collection(&self, id: &str) -> &Collection<Document> {
        self.entities.get(id).unwrap().as_collection()
    }

    pub fn get_session(&self, id: &str) -> &SessionEntity {
        self.entities.get(id).unwrap().as_session_entity()
    }

    pub fn get_mut_session(&mut self, id: &str) -> &mut SessionEntity {
        self.entities.get_mut(id).unwrap().as_mut_session_entity()
    }

    pub fn get_mut_find_cursor(&mut self, id: &str) -> &mut TestCursor {
        self.entities.get_mut(id).unwrap().as_mut_cursor()
    }

    pub fn write_events_list_to_file(&self, id: &str, writer: &mut BufWriter<File>) {
        let event_list_entity = match self.entities.get(id) {
            Some(entity) => entity.as_event_list(),
            None => return,
        };
        let client = self.get_client(&event_list_entity.client_id);
        let names: Vec<&str> = event_list_entity
            .event_names
            .iter()
            .map(String::as_ref)
            .collect();

        client.write_events_list_to_file(&names, writer);
    }
}
