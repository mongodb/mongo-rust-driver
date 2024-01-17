use std::{collections::HashMap, path::PathBuf, sync::Arc, time::Duration};

use futures::TryStreamExt;
use semver::Version;
use tokio::sync::{mpsc, RwLock};

use crate::{
    bson::{doc, Document},
    client::options::ClientOptions,
    concern::{Acknowledgment, WriteConcern},
    gridfs::GridFsBucket,
    options::{
        CollectionOptions,
        CreateCollectionOptions,
        FindOptions,
        ReadConcern,
        ReadPreference,
        SelectionCriteria,
    },
    runtime,
    sdam::{TopologyDescription, MIN_HEARTBEAT_FREQUENCY},
    test::{
        get_client_options,
        log_uncaptured,
        spec::unified_runner::{
            entity::EventList,
            matcher::events_match,
            test_file::{ExpectedEventType, TestFile},
        },
        update_options_for_testing,
        util::FailPointGuard,
        EventHandler,
        TestClient,
        DEFAULT_URI,
        LOAD_BALANCED_MULTIPLE_URI,
        LOAD_BALANCED_SINGLE_URI,
        SERVERLESS,
        SERVER_API,
    },
    Collection,
    Database,
};

use super::{
    entity::ThreadEntity,
    file_level_log,
    merge_uri_options,
    test_file::ThreadMessage,
    ClientEntity,
    CollectionData,
    Entity,
    SessionEntity,
    TestCursor,
    TestFileEntity,
};

#[cfg(feature = "in-use-encryption-unstable")]
use crate::test::KmsProviderList;

#[cfg(feature = "tracing-unstable")]
use crate::test::{
    spec::unified_runner::matcher::tracing_events_match,
    util::max_verbosity_levels_for_test_case,
    DEFAULT_GLOBAL_TRACING_HANDLER,
};

const SKIPPED_OPERATIONS: &[&str] = &[
    "bulkWrite",
    "count",
    "listCollectionObjects",
    "listDatabaseObjects",
    "mapReduce",
    "watch",
    "rewrapManyDataKey",
];

static MIN_SPEC_VERSION: Version = Version::new(1, 0, 0);
static MAX_SPEC_VERSION: Version = Version::new(1, 16, 0);

pub(crate) type EntityMap = HashMap<String, Entity>;

#[derive(Clone)]
pub(crate) struct TestRunner {
    pub(crate) internal_client: TestClient,
    pub(crate) entities: Arc<RwLock<EntityMap>>,
    pub(crate) fail_point_guards: Arc<RwLock<Vec<FailPointGuard>>>,
}

impl TestRunner {
    pub(crate) async fn new() -> Self {
        Self {
            internal_client: TestClient::new().await,
            entities: Default::default(),
            fail_point_guards: Default::default(),
        }
    }

    pub(crate) async fn new_with_connection_string(connection_string: &str) -> Self {
        #[cfg(all(not(feature = "sync"), not(feature = "tokio-sync")))]
        let options = ClientOptions::parse(connection_string).await.unwrap();
        #[cfg(any(feature = "sync", feature = "tokio-sync"))]
        let options = ClientOptions::parse(connection_string).unwrap();
        Self {
            internal_client: TestClient::with_options(Some(options)).await,
            entities: Arc::new(RwLock::new(EntityMap::new())),
            fail_point_guards: Arc::new(RwLock::new(Vec::new())),
        }
    }

    pub(crate) async fn run_test(
        &self,
        test_file: TestFile,
        path: impl Into<Option<PathBuf>>,
        skipped_tests: Option<&Vec<&str>>,
    ) {
        let schema_version = &test_file.schema_version;
        assert!(
            schema_version >= &MIN_SPEC_VERSION && schema_version <= &MAX_SPEC_VERSION,
            "Test runner not compatible with schema version {}",
            schema_version
        );

        let test_description = match path.into() {
            Some(path) => format!("{} ({:?})", &test_file.description, path),
            None => test_file.description.clone(),
        };

        if let Some(ref requirements) = test_file.run_on_requirements {
            let mut can_run_on = false;
            let mut run_on_errors = vec![];
            for requirement in requirements {
                match requirement.can_run_on(&self.internal_client).await {
                    Ok(()) => can_run_on = true,
                    Err(e) => run_on_errors.push(e),
                }
            }
            if !can_run_on {
                file_level_log(format!(
                    "Skipping {}: client topology not compatible with test ({})",
                    test_description,
                    run_on_errors.join(","),
                ));
                return;
            }
        }

        file_level_log(format!("Running tests from {}", test_description));

        for test_case in &test_file.tests {
            if let Ok(description) = std::env::var("TEST_DESCRIPTION") {
                if !test_case
                    .description
                    .to_lowercase()
                    .contains(&description.to_lowercase())
                {
                    continue;
                }
            }

            if let Some(skipped_tests) = skipped_tests {
                if skipped_tests.contains(&test_case.description.as_str()) {
                    log_uncaptured(format!(
                        "Skipping test case {}: test skipped manually",
                        &test_case.description
                    ));
                    continue;
                }
            }

            if let Some(skip_reason) = &test_case.skip_reason {
                log_uncaptured(format!(
                    "Skipping test case {:?}: {}",
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
                    "Skipping test case {:?}: unsupported operation {}",
                    &test_case.description, op
                ));
                continue;
            }

            if let Some(ref requirements) = test_case.run_on_requirements {
                let mut can_run_on = false;
                let mut run_on_errors = vec![];
                for requirement in requirements {
                    match requirement.can_run_on(&self.internal_client).await {
                        Ok(()) => can_run_on = true,
                        Err(e) => run_on_errors.push(e),
                    }
                }
                if !can_run_on {
                    log_uncaptured(format!(
                        "Skipping test case {:?}: client topology not compatible with test ({})",
                        &test_case.description,
                        run_on_errors.join(","),
                    ));
                    continue;
                }
            }

            log_uncaptured(format!("Executing {:?}", &test_case.description));

            if let Some(ref initial_data) = test_file.initial_data {
                for data in initial_data {
                    self.insert_initial_data(data).await;
                }
            }

            self.entities.write().await.clear();
            if let Some(ref create_entities) = test_file.create_entities {
                self.populate_entity_map(create_entities, &test_case.description)
                    .await;
            }

            #[cfg(feature = "tracing-unstable")]
            let (mut tracing_subscriber, _levels_guard) = {
                let tracing_levels =
                    max_verbosity_levels_for_test_case(&test_file.create_entities, test_case);
                let guard = DEFAULT_GLOBAL_TRACING_HANDLER.set_levels(tracing_levels);
                let subscriber = DEFAULT_GLOBAL_TRACING_HANDLER.subscribe();
                (subscriber, guard)
            };

            for operation in &test_case.operations {
                self.sync_workers().await;
                operation.execute(self, &test_case.description).await;
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
                    let entities = self.entities.read().await;
                    let entity = entities.get(&expected.client).unwrap();
                    let client = entity.as_client();
                    client.sync_workers().await;
                    let event_type = expected.event_type.unwrap_or(ExpectedEventType::Command);

                    let actual_events: Vec<_> =
                        client.get_filtered_events(event_type).into_iter().collect();

                    let expected_events = &expected.events;

                    if expected.ignore_extra_events.unwrap_or(false) {
                        assert!(
                            actual_events.len() >= expected_events.len(),
                            "[{}] actual:\n{:#?}\nexpected:\n{:#?}",
                            test_case.description,
                            actual_events,
                            expected_events
                        )
                    } else {
                        assert_eq!(
                            actual_events.len(),
                            expected_events.len(),
                            "[{}] actual:\n{:#?}\nexpected:\n{:#?}",
                            test_case.description,
                            actual_events,
                            expected_events
                        )
                    }

                    for (actual, expected) in actual_events.iter().zip(expected_events) {
                        if let Err(e) = events_match(actual, expected, Some(&entities)) {
                            panic!(
                                "event mismatch: expected = {:#?}, actual = {:#?}\nmismatch \
                                 detail: {}",
                                expected, actual, e,
                            );
                        }
                    }
                }
            }

            #[cfg(feature = "tracing-unstable")]
            if let Some(ref expected_messages) = test_case.expect_log_messages {
                self.sync_workers().await;

                let all_tracing_events = tracing_subscriber
                    .collect_events(Duration::from_millis(1000), |_| true)
                    .await;

                for expectation in expected_messages {
                    let client_topology_id = self.get_client(&expectation.client).await.topology_id;

                    let client_actual_events: Vec<_> = all_tracing_events
                        .iter()
                        .filter(|e| {
                            if e.topology_id() != client_topology_id.to_hex() {
                                return false;
                            }
                            if let Some(ref ignored_messages) = expectation.ignore_messages {
                                for ignored_message in ignored_messages {
                                    if tracing_events_match(e, ignored_message).is_ok() {
                                        return false;
                                    }
                                }
                            }
                            true
                        })
                        .collect();
                    let expected_events = &expectation.messages;

                    if expectation.ignore_extra_messages != Some(true) {
                        assert_eq!(
                            client_actual_events.len(),
                            expected_events.len(),
                            "Actual tracing event count should match expected. Expected events = \
                             {:#?}, actual events = {:#?}",
                            expected_events,
                            client_actual_events,
                        );
                    }

                    for (actual, expected) in client_actual_events.iter().zip(expected_events) {
                        if let Err(e) = tracing_events_match(actual, expected) {
                            panic!(
                                "tracing event mismatch: expected = {:#?}, actual = \
                                 {:#?}\nmismatch detail: {}",
                                expected, actual, e,
                            );
                        }
                    }
                }
            }

            self.fail_point_guards.write().await.clear();

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
        }
    }

    pub(crate) async fn insert_initial_data(&self, data: &CollectionData) {
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

    pub(crate) async fn populate_entity_map(
        &self,
        create_entities: &[TestFileEntity],
        description: impl AsRef<str>,
    ) {
        for entity in create_entities {
            let (id, entity) = match entity {
                TestFileEntity::Client(client) => {
                    if let Some(store_events_as_entities) = &client.store_events_as_entities {
                        for store_events_as_entity in store_events_as_entities {
                            let event_list = EventList {
                                client_id: client.id.clone(),
                                event_names: store_events_as_entity.events.clone(),
                            };
                            self.insert_entity(&store_events_as_entity.id, event_list)
                                .await;
                        }
                    }

                    let id = client.id.clone();
                    let observe_events = client.observe_events.clone();
                    let ignore_command_names = client.ignore_command_monitoring_events.clone();
                    let observe_sensitive_commands =
                        client.observe_sensitive_commands.unwrap_or(false);
                    let server_api = client.server_api.clone().or_else(|| SERVER_API.clone());

                    let given_uri = if get_client_options().await.load_balanced.unwrap_or(false) {
                        // for serverless testing, ignore use_multiple_mongoses.
                        if client.use_multiple_mongoses() && !*SERVERLESS {
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
                    let uri = merge_uri_options(
                        given_uri,
                        client.uri_options.as_ref(),
                        client.use_multiple_mongoses(),
                    );
                    let mut options =
                        ClientOptions::parse_uri(&uri, None)
                            .await
                            .unwrap_or_else(|e| {
                                panic!(
                                    "[{}] invalid client URI: {}, error: {}",
                                    description.as_ref(),
                                    uri,
                                    e
                                )
                            });
                    update_options_for_testing(&mut options);
                    let handler = Arc::new(EventHandler::new());
                    options.command_event_handler = Some(handler.clone().receive_command().into());
                    options.cmap_event_handler = Some(handler.clone());
                    options.sdam_event_handler = Some(handler.clone());

                    options.server_api = server_api;

                    if client.use_multiple_mongoses() && TestClient::new().await.is_sharded() {
                        assert!(
                            options.hosts.len() > 1,
                            "[{}]: Test requires multiple mongos hosts",
                            description.as_ref()
                        );
                    }

                    // In order to speed up the tests where a failpoint is used, the test runner
                    // MAY specified a reduced value for `heartbeatFrequencyMS` and
                    // `minHeartbeatFrequencyMS`. Test runners MUST NOT do so
                    // for any client that specifies `heartbeatFrequencyMS` it its
                    // `uriOptions`.
                    if options.heartbeat_freq.is_none() {
                        options.test_options_mut().min_heartbeat_freq =
                            Some(Duration::from_millis(50));
                        options.heartbeat_freq = Some(MIN_HEARTBEAT_FREQUENCY);
                    }

                    // if we're observing log messages, we need to set the entity ID on the test
                    // options so that it can be emitted in tracing events and
                    // used to filter events by client for test assertions.
                    #[cfg(feature = "tracing-unstable")]
                    if client.observe_log_messages.is_some() {
                        // some tests require that an untruncated command/reply is attached to
                        // events so it can be parsed as JSON, but on certain topologies some of
                        // the tests produce replies with extJSON longer than 1000 characters.
                        // to accomodate this, we use a higher default length for unified tests.
                        options.tracing_max_document_length_bytes = Some(10000);
                    }

                    (
                        id,
                        Entity::Client(ClientEntity::new(
                            options,
                            handler,
                            observe_events,
                            ignore_command_names,
                            observe_sensitive_commands,
                        )),
                    )
                }
                TestFileEntity::Database(database) => {
                    let id = database.id.clone();
                    let client = self.get_client(&database.client).await;
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
                    let database = self.get_database(&collection.database).await;
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
                    let client = self.get_client(&session.client).await;
                    let client_session = client
                        .start_session(session.session_options.clone())
                        .await
                        .unwrap();
                    (id, Entity::Session(SessionEntity::new(client_session)))
                }
                TestFileEntity::Bucket(bucket) => {
                    let id = bucket.id.clone();
                    let database = self.get_database(&bucket.database).await;
                    (
                        id,
                        Entity::Bucket(database.gridfs_bucket(bucket.bucket_options.clone())),
                    )
                }
                TestFileEntity::Thread(thread) => {
                    let (sender, mut receiver) = mpsc::unbounded_channel::<ThreadMessage>();
                    let runner = self.clone();
                    let d = description.as_ref().to_string();
                    runtime::execute(async move {
                        while let Some(msg) = receiver.recv().await {
                            match msg {
                                ThreadMessage::ExecuteOperation(op) => {
                                    op.execute(&runner, d.as_str()).await;
                                }
                                ThreadMessage::Stop(sender) => {
                                    // This returns an error if the waitForThread operation stopped
                                    // listening (e.g. due to timeout). The waitForThread operation
                                    // will handle reporting that error, so we can ignore it here.
                                    let _ = sender.send(());
                                    break;
                                }
                            }
                        }
                    });
                    (thread.id.clone(), Entity::Thread(ThreadEntity { sender }))
                }
                #[cfg(feature = "in-use-encryption-unstable")]
                TestFileEntity::ClientEncryption(client_enc) => {
                    let id = client_enc.id.clone();
                    let opts = &client_enc.client_encryption_opts;
                    let kv_client = self
                        .get_client(&opts.key_vault_client)
                        .await
                        .client()
                        .unwrap()
                        .clone();
                    let kms_providers: HashMap<mongocrypt::ctx::KmsProvider, Document> =
                        bson::from_document(opts.kms_providers.clone()).unwrap();
                    let kms_providers = fill_kms_placeholders(kms_providers);
                    let client_enc = crate::client_encryption::ClientEncryption::new(
                        kv_client,
                        opts.key_vault_namespace.clone(),
                        kms_providers,
                    )
                    .unwrap();
                    (id, Entity::ClientEncryption(Arc::new(client_enc)))
                }
            };
            self.insert_entity(&id, entity).await;
        }
    }

    pub(crate) async fn insert_entity(&self, id: impl AsRef<str>, entity: impl Into<Entity>) {
        if self
            .entities
            .write()
            .await
            .insert(id.as_ref().to_string(), entity.into())
            .is_some()
        {
            panic!(
                "Entity with id {} already present in entity map",
                id.as_ref()
            );
        }
    }

    pub(crate) async fn sync_workers(&self) {
        self.internal_client.sync_workers().await;
        let entities = self.entities.read().await;
        for entity in entities.values() {
            if let Entity::Client(client) = entity {
                client.sync_workers().await;
            }
        }
    }

    pub(crate) async fn get_client(&self, id: &str) -> ClientEntity {
        self.entities
            .read()
            .await
            .get(id)
            .unwrap()
            .as_client()
            .clone()
    }

    pub(crate) async fn get_database(&self, id: &str) -> Database {
        self.entities
            .read()
            .await
            .get(id)
            .unwrap()
            .as_database()
            .clone()
    }

    pub(crate) async fn get_collection(&self, id: &str) -> Collection<Document> {
        self.entities
            .read()
            .await
            .get(id)
            .unwrap()
            .as_collection()
            .clone()
    }

    pub(crate) async fn get_bucket(&self, id: &str) -> GridFsBucket {
        self.entities
            .read()
            .await
            .get(id)
            .unwrap()
            .as_bucket_entity()
            .clone()
    }

    pub(crate) async fn get_thread(&self, id: &str) -> ThreadEntity {
        self.entities
            .read()
            .await
            .get(id)
            .unwrap()
            .as_thread()
            .clone()
    }

    pub(crate) async fn get_topology_description(
        &self,
        id: impl AsRef<str>,
    ) -> TopologyDescription {
        self.entities
            .read()
            .await
            .get(id.as_ref())
            .unwrap()
            .as_topology_description()
            .clone()
    }

    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) async fn get_client_encryption(
        &self,
        id: impl AsRef<str>,
    ) -> Arc<crate::client_encryption::ClientEncryption> {
        self.entities
            .read()
            .await
            .get(id.as_ref())
            .unwrap()
            .as_client_encryption()
            .clone()
    }

    /// Removes the cursor with the given ID from the entity map. This method passes ownership of
    /// the cursor to the caller so that a mutable reference to a ClientSession can be accessed from
    /// the entity map simultaneously. Once the caller is finished with the cursor, it MUST be
    /// returned to the test runner via the return_cursor method below.
    pub(crate) async fn take_cursor(&self, id: impl AsRef<str>) -> TestCursor {
        self.entities
            .write()
            .await
            .remove(id.as_ref())
            .unwrap()
            .into_cursor()
    }

    /// Returns the given cursor to the entity map. This method must be called after take_cursor.
    pub(crate) async fn return_cursor(&self, id: impl AsRef<str>, cursor: TestCursor) {
        self.entities
            .write()
            .await
            .insert(id.as_ref().into(), Entity::Cursor(cursor));
    }
}

#[cfg(feature = "in-use-encryption-unstable")]
fn fill_kms_placeholders(
    kms_providers: HashMap<mongocrypt::ctx::KmsProvider, Document>,
) -> KmsProviderList {
    use crate::test::KMS_PROVIDERS_MAP;

    let placeholder = bson::Bson::Document(doc! { "$$placeholder": 1 });

    let mut out = vec![];
    for (provider, mut config) in kms_providers {
        for (key, value) in config.iter_mut() {
            if *value == placeholder {
                let new_value = KMS_PROVIDERS_MAP
                    .get(&provider)
                    .unwrap_or_else(|| panic!("missing config for {:?}", provider))
                    .0
                    .get(key)
                    .unwrap_or_else(|| {
                        panic!("provider config {:?} missing key {:?}", provider, key)
                    })
                    .clone();
                *value = new_value;
            }
        }
        let tls = KMS_PROVIDERS_MAP
            .get(&provider)
            .and_then(|(_, t)| t.clone());
        out.push((provider, config, tls));
    }
    out
}
