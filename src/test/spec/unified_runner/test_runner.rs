use std::{collections::HashMap, sync::Arc};

use crate::{
    bson::Document,
    client::options::ClientOptions,
    concern::{Acknowledgment, WriteConcern},
    db::options::CreateCollectionOptions,
    options::CollectionOptions,
    test::{
        util::FailPointGuard,
        EventHandler,
        TestClient,
        CLIENT_OPTIONS,
        DEFAULT_URI,
        LOAD_BALANCED_MULTIPLE_URI,
        LOAD_BALANCED_SINGLE_URI,
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
    FindCursor,
    SessionEntity,
    TestFileEntity,
};

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
                    let id = client.id.clone();
                    let observe_events = client.observe_events.clone();
                    let ignore_command_names = client.ignore_command_monitoring_events.clone();
                    let observe_sensitive_commands =
                        client.observe_sensitive_commands.unwrap_or(false);
                    let server_api = client.server_api.clone().or_else(|| SERVER_API.clone());
                    let observer = Arc::new(EventHandler::new());

                    let given_uri = if CLIENT_OPTIONS.load_balanced.unwrap_or(false) {
                        if client.use_multiple_mongoses.unwrap_or(true) {
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
                    options.command_event_handler = Some(observer.clone());
                    options.cmap_event_handler = Some(observer.clone());
                    options.server_api = server_api;
                    if LOAD_BALANCED_SINGLE_URI
                        .as_ref()
                        .map_or(false, |uri| !uri.is_empty())
                    {
                        options.test_options_mut().mock_service_id = true;
                    }
                    if TestClient::new().await.is_sharded() {
                        match client.use_multiple_mongoses {
                            Some(true) => {
                                if options.hosts.len() <= 1 {
                                    panic!("Test requires multiple mongos hosts");
                                }
                            }
                            Some(false) => {
                                options.hosts.drain(1..);
                            }
                            None => {}
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
            if self.entities.insert(id.clone(), entity).is_some() {
                panic!("Entity with id {} already present in entity map", id);
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

    pub fn get_mut_find_cursor(&mut self, id: &str) -> &mut FindCursor {
        self.entities.get_mut(id).unwrap().as_mut_find_cursor()
    }
}
