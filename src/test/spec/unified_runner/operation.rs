mod bulk_write;
mod collection_operations;
mod command_operations;
mod connection_operations;
mod count_operations;
#[cfg(feature = "in-use-encryption")]
mod csfle;
mod delete_operations;
mod failpoint_operations;
mod find_operations;
mod index_operations;
mod insert_operations;
mod iteration_operations;
mod list_operations;
mod search_index;
mod session_operations;
mod thread_operations;
mod topology_operations;
mod transaction_operations;
mod update_operations;
mod upload_download_operations;
mod wait_operations;

use std::{
    fmt::Debug,
    ops::Deref,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use collection_operations::{
    AssertCollectionExists,
    AssertCollectionNotExists,
    CreateCollection,
    DropCollection,
    RenameCollection,
};
use command_operations::{CreateCommandCursor, RunCommand, RunCursorCommand};
use connection_operations::{AssertNumberConnectionsCheckedOut, Close};
use count_operations::{
    Aggregate,
    AssertEventCount,
    CountDocuments,
    Distinct,
    EstimatedDocumentCount,
};
use delete_operations::{DeleteMany, DeleteOne};
use failpoint_operations::{FailPointCommand, TargetedFailPoint};
use find_operations::{
    CreateFindCursor,
    Find,
    FindOne,
    FindOneAndDelete,
    FindOneAndReplace,
    FindOneAndUpdate,
};
use futures::{future::BoxFuture, FutureExt};
use index_operations::{
    AssertIndexExists,
    AssertIndexNotExists,
    CreateIndex,
    DropIndex,
    ListIndexNames,
    ListIndexes,
};
use insert_operations::{InsertMany, InsertOne};
use iteration_operations::{IterateOnce, IterateUntilDocumentOrError};
use list_operations::{ListCollectionNames, ListCollections, ListDatabaseNames, ListDatabases};
use serde::{
    de::{DeserializeOwned, Deserializer},
    Deserialize,
};
use session_operations::{
    AssertDifferentLsidOnLastTwoCommands,
    AssertSameLsidOnLastTwoCommands,
    AssertSessionDirty,
    AssertSessionNotDirty,
    AssertSessionPinned,
    AssertSessionTransactionState,
    AssertSessionUnpinned,
    EndSession,
};
use thread_operations::{RunOnThread, WaitForThread};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use topology_operations::{AssertTopologyType, RecordTopologyDescription};
use transaction_operations::{
    AbortTransaction,
    CommitTransaction,
    StartTransaction,
    WithTransaction,
};
use update_operations::{ReplaceOne, UpdateMany, UpdateOne};
use upload_download_operations::{Delete, Download, DownloadByName, Upload};
use wait_operations::{Wait, WaitForEvent, WaitForPrimaryChange};

use super::{
    results_match,
    Entity,
    EntityMap,
    ExpectError,
    TestCursor,
    TestFileEntity,
    TestRunner,
};

use crate::{
    bson::{doc, Bson, Document},
    error::{ErrorKind, Result},
    options::ChangeStreamOptions,
};

use bulk_write::*;
#[cfg(feature = "in-use-encryption")]
use csfle::*;
use search_index::*;

pub(crate) trait TestOperation: Debug + Send + Sync {
    fn execute_test_runner_operation<'a>(
        &'a self,
        _test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        panic!(
            "execute_test_runner_operation called on unsupported operation {:?}",
            self
        )
    }

    fn execute_entity_operation<'a>(
        &'a self,
        _id: &'a str,
        _test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            Err(ErrorKind::InvalidArgument {
                message: format!(
                    "execute_entity_operation called on unsupported operation {:?}",
                    self
                ),
            }
            .into())
        }
        .boxed()
    }

    /// Whether or not this operation returns an array of root documents. This information is
    /// necessary to determine how the return value of an operation should be compared to the
    /// expected value.
    fn returns_root_documents(&self) -> bool {
        false
    }

    /// If this operation specifies entities to create, returns those entities. Otherwise,
    /// returns None.
    #[cfg(feature = "tracing-unstable")]
    fn test_file_entities(&self) -> Option<&Vec<TestFileEntity>> {
        None
    }
}

/// To facilitate working with sessions through the lock, this macro pops it out of the entity map,
/// "passes" it to the provided block, and then returns it to the entity map. It does it this way
/// so that we can continue to borrow the entity map in other ways even when we're using a session,
/// which we'd have to borrow mutably from the map.
macro_rules! with_mut_session {
    ($test_runner:ident, $id:expr, |$session:ident| $body:expr) => {
        async {
            let id = $id;
            let entity = $test_runner.entities.write().await.remove(id).unwrap();
            match entity {
                Entity::Session(mut session_owned) => {
                    let $session: &mut crate::ClientSession = &mut session_owned;
                    let out = $body.await;
                    $test_runner
                        .entities
                        .write()
                        .await
                        .insert(id.to_string(), Entity::Session(session_owned));
                    out
                }
                Entity::SessionPtr(ptr) => {
                    let $session = unsafe { &mut *ptr.0 };
                    let out = $body.await;
                    $test_runner
                        .entities
                        .write()
                        .await
                        .insert(id.to_string(), Entity::SessionPtr(ptr));
                    out
                }
                o => panic!(
                    "expected {} to be a session entity, instead was {:?}",
                    $id, o
                ),
            }
        }
    };
}
use with_mut_session;

macro_rules! with_opt_session {
    ($test_runner:ident, $id:expr, $act:expr $(,)?) => {
        async {
            let act = $act;
            match $id {
                Some(id) => {
                    with_mut_session!($test_runner, id, |session| act.session(session)).await
                }
                None => act.await,
            }
        }
    };
}
use with_opt_session;

#[derive(Debug)]
pub(crate) struct Operation {
    operation: Box<dyn TestOperation>,
    pub(crate) name: String,
    pub(crate) object: OperationObject,
    pub(crate) expectation: Expectation,
}

impl Operation {
    pub(crate) async fn execute(&self, test_runner: &TestRunner, description: &str) {
        let _ = self.execute_fallible(test_runner, description).await;
    }

    async fn execute_fallible(&self, test_runner: &TestRunner, description: &str) -> Result<()> {
        match self.object {
            OperationObject::TestRunner => {
                self.execute_test_runner_operation(test_runner).await;
                Ok(())
            }
            OperationObject::Entity(ref id) => {
                let result = self.execute_entity_operation(id, test_runner).await;
                let error = result.as_ref().map_or_else(|e| Err(e.clone()), |_| Ok(()));

                match &self.expectation {
                    Expectation::Result {
                        expected_value,
                        save_as_entity,
                    } => {
                        let opt_entity = result.unwrap_or_else(|e| {
                            panic!(
                                "[{}] {} should succeed, but failed with the following error: {}",
                                description, self.name, e
                            )
                        });
                        if expected_value.is_some() || save_as_entity.is_some() {
                            let entity = opt_entity.unwrap_or_else(|| {
                                panic!("[{}] {} did not return an entity", description, self.name)
                            });
                            if let Some(expected_bson) = expected_value {
                                let actual = match &entity {
                                    Entity::Bson(bs) => Some(bs),
                                    Entity::None => None,
                                    _ => panic!(
                                        "[{}] Incorrect entity type returned from {}, expected \
                                         BSON",
                                        description, self.name
                                    ),
                                };
                                if let Err(e) = results_match(
                                    actual,
                                    expected_bson,
                                    self.returns_root_documents(),
                                    Some(&*test_runner.entities.read().await),
                                ) {
                                    panic!(
                                        "[{}] result mismatch, expected = {:#?}  actual = \
                                         {:#?}\nmismatch detail: {}",
                                        description, expected_bson, actual, e
                                    );
                                }
                            }
                            if let Some(id) = save_as_entity {
                                test_runner.insert_entity(id, entity).await;
                            }
                        }
                    }
                    Expectation::Error(expect_error) => {
                        let error = result.expect_err(&format!(
                            "{}: {} should return an error",
                            description, self.name
                        ));
                        expect_error.verify_result(&error, description);
                    }
                    Expectation::Ignore => (),
                }
                error
            }
        }
    }
}

#[derive(Debug)]
pub(crate) enum OperationObject {
    TestRunner,
    Entity(String),
}

impl<'de> Deserialize<'de> for OperationObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let object = String::deserialize(deserializer)?;
        if object.as_str() == "testRunner" {
            Ok(OperationObject::TestRunner)
        } else {
            Ok(OperationObject::Entity(object))
        }
    }
}

#[derive(Debug)]
pub(crate) enum Expectation {
    Result {
        expected_value: Option<Bson>,
        save_as_entity: Option<String>,
    },
    Error(ExpectError),
    Ignore,
}

fn deserialize_op<'de, 'a, T: 'a + DeserializeOwned + TestOperation>(
    value: Document,
) -> std::result::Result<Box<dyn TestOperation + 'a>, bson::de::Error> {
    bson::from_document::<T>(value).map(|op| Box::new(op) as Box<dyn TestOperation>)
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OperationDefinition {
            pub(crate) name: String,
            pub(crate) object: OperationObject,
            #[serde(default = "Document::new")]
            pub(crate) arguments: Document,
            pub(crate) expect_error: Option<ExpectError>,
            pub(crate) expect_result: Option<Bson>,
            pub(crate) save_result_as_entity: Option<String>,
            pub(crate) ignore_result_and_error: Option<bool>,
        }

        let definition = OperationDefinition::deserialize(deserializer)?;
        let boxed_op = match definition.name.as_str() {
            "insertOne" => deserialize_op::<InsertOne>(definition.arguments),
            "insertMany" => deserialize_op::<InsertMany>(definition.arguments),
            "updateOne" => deserialize_op::<UpdateOne>(definition.arguments),
            "updateMany" => deserialize_op::<UpdateMany>(definition.arguments),
            "deleteMany" => deserialize_op::<DeleteMany>(definition.arguments),
            "deleteOne" => deserialize_op::<DeleteOne>(definition.arguments),
            "find" => deserialize_op::<Find>(definition.arguments),
            "createFindCursor" => deserialize_op::<CreateFindCursor>(definition.arguments),
            "createCommandCursor" => deserialize_op::<CreateCommandCursor>(definition.arguments),
            "aggregate" => deserialize_op::<Aggregate>(definition.arguments),
            "distinct" => deserialize_op::<Distinct>(definition.arguments),
            "countDocuments" => deserialize_op::<CountDocuments>(definition.arguments),
            "estimatedDocumentCount" => {
                deserialize_op::<EstimatedDocumentCount>(definition.arguments)
            }
            "findOne" => deserialize_op::<FindOne>(definition.arguments),
            "listDatabases" => deserialize_op::<ListDatabases>(definition.arguments),
            "listDatabaseNames" => deserialize_op::<ListDatabaseNames>(definition.arguments),
            "listCollections" => deserialize_op::<ListCollections>(definition.arguments),
            "listCollectionNames" => deserialize_op::<ListCollectionNames>(definition.arguments),
            "replaceOne" => deserialize_op::<ReplaceOne>(definition.arguments),
            "findOneAndUpdate" => deserialize_op::<FindOneAndUpdate>(definition.arguments),
            "findOneAndReplace" => deserialize_op::<FindOneAndReplace>(definition.arguments),
            "findOneAndDelete" => deserialize_op::<FindOneAndDelete>(definition.arguments),
            "failPoint" => deserialize_op::<FailPointCommand>(definition.arguments),
            "targetedFailPoint" => deserialize_op::<TargetedFailPoint>(definition.arguments),
            "assertCollectionExists" => {
                deserialize_op::<AssertCollectionExists>(definition.arguments)
            }
            "assertCollectionNotExists" => {
                deserialize_op::<AssertCollectionNotExists>(definition.arguments)
            }
            "createCollection" => deserialize_op::<CreateCollection>(definition.arguments),
            "dropCollection" => deserialize_op::<DropCollection>(definition.arguments),
            "runCommand" => deserialize_op::<RunCommand>(definition.arguments),
            "runCursorCommand" => deserialize_op::<RunCursorCommand>(definition.arguments),
            "endSession" => deserialize_op::<EndSession>(definition.arguments),
            "assertSessionTransactionState" => {
                deserialize_op::<AssertSessionTransactionState>(definition.arguments)
            }
            "assertSessionPinned" => deserialize_op::<AssertSessionPinned>(definition.arguments),
            "assertSessionUnpinned" => {
                deserialize_op::<AssertSessionUnpinned>(definition.arguments)
            }
            "assertDifferentLsidOnLastTwoCommands" => {
                deserialize_op::<AssertDifferentLsidOnLastTwoCommands>(definition.arguments)
            }
            "assertSameLsidOnLastTwoCommands" => {
                deserialize_op::<AssertSameLsidOnLastTwoCommands>(definition.arguments)
            }
            "assertSessionDirty" => deserialize_op::<AssertSessionDirty>(definition.arguments),
            "assertSessionNotDirty" => {
                deserialize_op::<AssertSessionNotDirty>(definition.arguments)
            }
            "startTransaction" => deserialize_op::<StartTransaction>(definition.arguments),
            "commitTransaction" => deserialize_op::<CommitTransaction>(definition.arguments),
            "abortTransaction" => deserialize_op::<AbortTransaction>(definition.arguments),
            "withTransaction" => deserialize_op::<WithTransaction>(definition.arguments),
            "createIndex" => deserialize_op::<CreateIndex>(definition.arguments),
            "listIndexes" => deserialize_op::<ListIndexes>(definition.arguments),
            "listIndexNames" => deserialize_op::<ListIndexNames>(definition.arguments),
            "assertIndexExists" => deserialize_op::<AssertIndexExists>(definition.arguments),
            "assertIndexNotExists" => deserialize_op::<AssertIndexNotExists>(definition.arguments),
            "iterateUntilDocumentOrError" => {
                deserialize_op::<IterateUntilDocumentOrError>(definition.arguments)
            }
            "assertNumberConnectionsCheckedOut" => {
                deserialize_op::<AssertNumberConnectionsCheckedOut>(definition.arguments)
            }
            "close" => deserialize_op::<Close>(definition.arguments),
            "createChangeStream" => deserialize_op::<CreateChangeStream>(definition.arguments),
            "rename" => deserialize_op::<RenameCollection>(definition.arguments),
            "loop" => deserialize_op::<Loop>(definition.arguments),
            "waitForEvent" => deserialize_op::<WaitForEvent>(definition.arguments),
            "assertEventCount" => deserialize_op::<AssertEventCount>(definition.arguments),
            "runOnThread" => deserialize_op::<RunOnThread>(definition.arguments),
            "waitForThread" => deserialize_op::<WaitForThread>(definition.arguments),
            "recordTopologyDescription" => {
                deserialize_op::<RecordTopologyDescription>(definition.arguments)
            }
            "assertTopologyType" => deserialize_op::<AssertTopologyType>(definition.arguments),
            "waitForPrimaryChange" => deserialize_op::<WaitForPrimaryChange>(definition.arguments),
            "wait" => deserialize_op::<Wait>(definition.arguments),
            "createEntities" => deserialize_op::<CreateEntities>(definition.arguments),
            "download" => deserialize_op::<Download>(definition.arguments),
            "downloadByName" => deserialize_op::<DownloadByName>(definition.arguments),
            "delete" => deserialize_op::<Delete>(definition.arguments),
            "upload" => deserialize_op::<Upload>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "getKeyByAltName" => deserialize_op::<GetKeyByAltName>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "deleteKey" => deserialize_op::<DeleteKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "getKey" => deserialize_op::<GetKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "addKeyAltName" => deserialize_op::<AddKeyAltName>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "createDataKey" => deserialize_op::<CreateDataKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "getKeys" => deserialize_op::<GetKeys>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "removeKeyAltName" => deserialize_op::<RemoveKeyAltName>(definition.arguments),
            "iterateOnce" => deserialize_op::<IterateOnce>(definition.arguments),
            "createSearchIndex" => deserialize_op::<CreateSearchIndex>(definition.arguments),
            "createSearchIndexes" => deserialize_op::<CreateSearchIndexes>(definition.arguments),
            "dropSearchIndex" => deserialize_op::<DropSearchIndex>(definition.arguments),
            "listSearchIndexes" => deserialize_op::<ListSearchIndexes>(definition.arguments),
            "updateSearchIndex" => deserialize_op::<UpdateSearchIndex>(definition.arguments),
            "clientBulkWrite" => deserialize_op::<BulkWrite>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "encrypt" => deserialize_op::<Encrypt>(definition.arguments),
            #[cfg(feature = "in-use-encryption")]
            "decrypt" => deserialize_op::<Decrypt>(definition.arguments),
            "dropIndex" => deserialize_op::<DropIndex>(definition.arguments),
            s => Ok(Box::new(UnimplementedOperation {
                _name: s.to_string(),
            }) as Box<dyn TestOperation>),
        }
        .map_err(|e| serde::de::Error::custom(format!("{}", e)))?;

        let expectation = if let Some(true) = definition.ignore_result_and_error {
            if definition.expect_result.is_some()
                || definition.expect_error.is_some()
                || definition.save_result_as_entity.is_some()
            {
                return Err(serde::de::Error::custom(
                    "ignoreResultAndError is mutually exclusive with expectResult, expectError, \
                     and saveResultAsEntity",
                ));
            }
            Expectation::Ignore
        } else if let Some(err) = definition.expect_error {
            if definition.expect_result.is_some() || definition.save_result_as_entity.is_some() {
                return Err(serde::de::Error::custom(
                    "expectError is mutually exclusive with expectResult and saveResultAsEntity",
                ));
            }
            Expectation::Error(err)
        } else {
            Expectation::Result {
                expected_value: definition.expect_result,
                save_as_entity: definition.save_result_as_entity,
            }
        };

        Ok(Operation {
            operation: boxed_op,
            name: definition.name,
            object: definition.object,
            expectation,
        })
    }
}

impl Deref for Operation {
    type Target = Box<dyn TestOperation>;

    fn deref(&self) -> &Box<dyn TestOperation> {
        &self.operation
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateChangeStream {
    pipeline: Vec<Document>,
    #[serde(flatten)]
    options: Option<ChangeStreamOptions>,
}

impl TestOperation for CreateChangeStream {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let entities = test_runner.entities.read().await;
            let target = entities.get(id).unwrap();
            let stream = match target {
                Entity::Client(ce) => {
                    ce.watch()
                        .pipeline(self.pipeline.clone())
                        .with_options(self.options.clone())
                        .await?
                }
                Entity::Database(db) => {
                    db.watch()
                        .pipeline(self.pipeline.clone())
                        .with_options(self.options.clone())
                        .await?
                }
                Entity::Collection(coll) => {
                    coll.watch()
                        .pipeline(self.pipeline.clone())
                        .with_options(self.options.clone())
                        .await?
                }
                _ => panic!("Invalid entity for createChangeStream"),
            };
            Ok(Some(Entity::Cursor(TestCursor::ChangeStream(Mutex::new(
                stream.with_type::<Document>(),
            )))))
        }
        .boxed()
    }
}

macro_rules! report_error {
    ($loop:expr, $error:expr, $entities:expr) => {{
        let error = format!("{:?}", $error);
        report_error_or_failure!(
            $loop.store_errors_as_entity,
            $loop.store_failures_as_entity,
            error,
            $entities
        );
    }};
}

macro_rules! report_failure {
    ($loop:expr, $name:expr, $actual:expr, $expected:expr, $entities:expr) => {{
        let error = format!(
            "{} error: got {:?}, expected {:?}",
            $name, $actual, $expected
        );
        report_error_or_failure!(
            $loop.store_failures_as_entity,
            $loop.store_errors_as_entity,
            error,
            $entities
        );
    }};
}

macro_rules! report_error_or_failure {
    ($first_option:expr, $second_option:expr, $error:expr, $entities:expr) => {{
        let id = if let Some(ref id) = $first_option {
            id
        } else if let Some(ref id) = $second_option {
            id
        } else {
            panic!(
                "At least one of storeErrorsAsEntity and storeFailuresAsEntity must be specified \
                 for a loop operation"
            );
        };

        match $entities.get_mut(id) {
            Some(Entity::Bson(Bson::Array(array))) => {
                let doc = doc! {
                    "error": $error,
                    "time": OffsetDateTime::now_utc().unix_timestamp(),
                };
                array.push(doc.into());
            }
            _ => panic!("Test runner should contain a Bson::Array entity for {}", id),
        };

        // The current iteration should end if an error or failure is encountered.
        break;
    }};
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Loop {
    operations: Vec<Operation>,
    store_errors_as_entity: Option<String>,
    store_failures_as_entity: Option<String>,
    store_successes_as_entity: Option<String>,
    store_iterations_as_entity: Option<String>,
}

impl TestOperation for Loop {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            if let Some(id) = &self.store_errors_as_entity {
                let errors = Bson::Array(vec![]);
                test_runner.insert_entity(id, errors).await;
            }
            if let Some(id) = &self.store_failures_as_entity {
                let failures = Bson::Array(vec![]);
                test_runner.insert_entity(id, failures).await;
            }
            if let Some(id) = &self.store_successes_as_entity {
                let successes = Bson::Int64(0);
                test_runner.insert_entity(id, successes).await;
            }
            if let Some(id) = &self.store_iterations_as_entity {
                let iterations = Bson::Int64(0);
                test_runner.insert_entity(id, iterations).await;
            }

            let continue_looping = Arc::new(AtomicBool::new(true));
            let continue_looping_handle = continue_looping.clone();
            ctrlc::set_handler(move || {
                continue_looping_handle.store(false, Ordering::SeqCst);
            })
            .expect("Failed to set ctrl-c handler");

            while continue_looping.load(Ordering::SeqCst) {
                for operation in &self.operations {
                    let result = match operation.object {
                        OperationObject::TestRunner => {
                            panic!("Operations within a loop must be entity operations")
                        }
                        OperationObject::Entity(ref id) => {
                            operation.execute_entity_operation(id, test_runner).await
                        }
                    };

                    let mut entities = test_runner.entities.write().await;
                    match (result, &operation.expectation) {
                        (
                            Ok(entity),
                            Expectation::Result {
                                expected_value,
                                save_as_entity,
                            },
                        ) => {
                            if let Some(expected_value) = expected_value {
                                let actual_value = match entity {
                                    Some(Entity::Bson(ref actual_value)) => Some(actual_value),
                                    None => None,
                                    _ => {
                                        report_failure!(
                                            self,
                                            &operation.name,
                                            entity,
                                            expected_value,
                                            &mut entities
                                        );
                                    }
                                };
                                if results_match(
                                    actual_value,
                                    expected_value,
                                    operation.returns_root_documents(),
                                    Some(&entities),
                                )
                                .is_ok()
                                {
                                    self.report_success(&mut entities);
                                } else {
                                    report_failure!(
                                        self,
                                        &operation.name,
                                        actual_value,
                                        expected_value,
                                        &mut entities
                                    );
                                }
                            } else {
                                self.report_success(&mut entities);
                            }
                            if let (Some(entity), Some(id)) = (entity, save_as_entity) {
                                entities.insert(id.to_string(), entity);
                            }
                        }
                        (Ok(result), Expectation::Error(ref expected_error)) => {
                            report_failure!(
                                self,
                                &operation.name,
                                result,
                                expected_error,
                                &mut entities
                            );
                        }
                        (Ok(_), Expectation::Ignore) => {
                            self.report_success(&mut entities);
                        }
                        (Err(error), Expectation::Error(ref expected_error)) => {
                            match catch_unwind(AssertUnwindSafe(|| {
                                expected_error.verify_result(&error, operation.name.as_str())
                            })) {
                                Ok(_) => self.report_success(&mut entities),
                                Err(_) => report_error_or_failure!(
                                    self.store_failures_as_entity,
                                    self.store_errors_as_entity,
                                    format!("expected {:?}, got {:?}", expected_error, error),
                                    &mut entities
                                ),
                            }
                        }
                        (Err(error), Expectation::Result { .. } | Expectation::Ignore) => {
                            report_error!(self, error, &mut entities);
                        }
                    }
                }
                let mut entities = test_runner.entities.write().await;
                self.report_iteration(&mut entities);
            }
        }
        .boxed()
    }
}

impl Loop {
    fn report_iteration(&self, entities: &mut EntityMap) {
        Self::increment_count(self.store_iterations_as_entity.as_ref(), entities)
    }

    fn report_success(&self, test_runner: &mut EntityMap) {
        Self::increment_count(self.store_successes_as_entity.as_ref(), test_runner)
    }

    fn increment_count(id: Option<&String>, entities: &mut EntityMap) {
        if let Some(id) = id {
            match entities.get_mut(id) {
                Some(Entity::Bson(Bson::Int64(count))) => *count += 1,
                _ => panic!("Test runner should contain a Bson::Int64 entity for {}", id),
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateEntities {
    entities: Vec<TestFileEntity>,
}

impl TestOperation for CreateEntities {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        test_runner
            .populate_entity_map(&self.entities[..], "createEntities operation")
            .boxed()
    }

    #[cfg(feature = "tracing-unstable")]
    fn test_file_entities(&self) -> Option<&Vec<TestFileEntity>> {
        Some(&self.entities)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation {
    _name: String,
}

impl TestOperation for UnimplementedOperation {}
