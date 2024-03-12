mod bulk_write;
#[cfg(feature = "in-use-encryption-unstable")]
mod csfle;
mod search_index;

use std::{
    collections::HashMap,
    convert::TryInto,
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{
    future::BoxFuture,
    io::AsyncReadExt,
    stream::{StreamExt, TryStreamExt},
    FutureExt,
};
use serde::{
    de::{DeserializeOwned, Deserializer},
    Deserialize,
};
use time::OffsetDateTime;
use tokio::sync::Mutex;

use super::{
    results_match,
    Entity,
    EntityMap,
    ExpectError,
    ExpectedEvent,
    TestCursor,
    TestFileEntity,
    TestRunner,
};

use crate::{
    action::Action,
    bson::{doc, to_bson, Bson, Document},
    client::session::TransactionState,
    coll::options::Hint,
    collation::Collation,
    db::options::{ListCollectionsOptions, RunCursorCommandOptions},
    error::{ErrorKind, Result},
    gridfs::options::{GridFsDownloadByNameOptions, GridFsUploadOptions},
    options::{
        AggregateOptions,
        ChangeStreamOptions,
        CountOptions,
        CreateCollectionOptions,
        DeleteOptions,
        DistinctOptions,
        DropCollectionOptions,
        EstimatedDocumentCountOptions,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        FindOptions,
        IndexOptions,
        InsertManyOptions,
        InsertOneOptions,
        ListIndexesOptions,
        ReadConcern,
        ReplaceOptions,
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
    },
    runtime,
    selection_criteria::ReadPreference,
    serde_util,
    test::FailPoint,
    Collection,
    Database,
    IndexModel,
    ServerType,
    TopologyType,
};

use bulk_write::*;
#[cfg(feature = "in-use-encryption-unstable")]
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
            let mut session_owned = match $test_runner.entities.write().await.remove(id).unwrap() {
                Entity::Session(session_owned) => session_owned,
                o => panic!(
                    "expected {} to be a session entity, instead was {:?}",
                    $id, o
                ),
            };
            let $session = &mut session_owned;
            let out = $body.await;
            $test_runner
                .entities
                .write()
                .await
                .insert(id.to_string(), Entity::Session(session_owned));
            out
        }
    };
}
use with_mut_session;

#[derive(Debug)]
pub(crate) struct Operation {
    operation: Box<dyn TestOperation>,
    pub(crate) name: String,
    pub(crate) object: OperationObject,
    pub(crate) expectation: Expectation,
}

impl Operation {
    pub(crate) async fn execute<'a>(&self, test_runner: &TestRunner, description: &str) {
        match self.object {
            OperationObject::TestRunner => {
                self.execute_test_runner_operation(test_runner).await;
            }
            OperationObject::Entity(ref id) => {
                let result = self.execute_entity_operation(id, test_runner).await;

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
                        expect_error.verify_result(&error, description).unwrap();
                    }
                    Expectation::Ignore => (),
                }
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
            #[cfg(feature = "in-use-encryption-unstable")]
            "getKeyByAltName" => deserialize_op::<GetKeyByAltName>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "deleteKey" => deserialize_op::<DeleteKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "getKey" => deserialize_op::<GetKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "addKeyAltName" => deserialize_op::<AddKeyAltName>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "createDataKey" => deserialize_op::<CreateDataKey>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "getKeys" => deserialize_op::<GetKeys>(definition.arguments),
            #[cfg(feature = "in-use-encryption-unstable")]
            "removeKeyAltName" => deserialize_op::<RemoveKeyAltName>(definition.arguments),
            "iterateOnce" => deserialize_op::<IterateOnce>(definition.arguments),
            "createSearchIndex" => deserialize_op::<CreateSearchIndex>(definition.arguments),
            "createSearchIndexes" => deserialize_op::<CreateSearchIndexes>(definition.arguments),
            "dropSearchIndex" => deserialize_op::<DropSearchIndex>(definition.arguments),
            "listSearchIndexes" => deserialize_op::<ListSearchIndexes>(definition.arguments),
            "updateSearchIndex" => deserialize_op::<UpdateSearchIndex>(definition.arguments),
            "clientBulkWrite" => deserialize_op::<BulkWrite>(definition.arguments),
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
pub(super) struct DeleteMany {
    filter: Document,
    #[serde(flatten)]
    options: DeleteOptions,
}

impl TestOperation for DeleteMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .delete_many(self.filter.clone())
                .with_options(self.options.clone())
                .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteOne {
    filter: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: DeleteOptions,
}

impl TestOperation for DeleteOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection
                .delete_one(self.filter.clone())
                .with_options(self.options.clone());
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        act.session(session.deref_mut()).await
                    })
                    .await?
                }
                None => act.await?,
            };
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Find {
    filter: Document,
    session: Option<String>,
    // `FindOptions` cannot be embedded directly because serde doesn't support combining `flatten`
    // and `deny_unknown_fields`, so its fields are replicated here.
    allow_disk_use: Option<bool>,
    allow_partial_results: Option<bool>,
    batch_size: Option<u32>,
    comment: Option<Bson>,
    hint: Option<Hint>,
    limit: Option<i64>,
    max: Option<Document>,
    max_scan: Option<u64>,
    #[serde(
        default,
        rename = "maxTimeMS",
        deserialize_with = "serde_util::deserialize_duration_option_from_u64_millis"
    )]
    max_time: Option<Duration>,
    min: Option<Document>,
    no_cursor_timeout: Option<bool>,
    projection: Option<Document>,
    read_concern: Option<ReadConcern>,
    return_key: Option<bool>,
    show_record_id: Option<bool>,
    skip: Option<u64>,
    sort: Option<Document>,
    collation: Option<Collation>,
    #[serde(rename = "let")]
    let_vars: Option<Document>,
}

impl Find {
    async fn get_cursor<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> Result<TestCursor> {
        let collection = test_runner.get_collection(id).await;

        let (comment, comment_bson) = match &self.comment {
            Some(Bson::String(string)) => (Some(string.clone()), None),
            Some(bson) => (None, Some(bson.clone())),
            None => (None, None),
        };

        // `FindOptions` is constructed without the use of `..Default::default()` to enforce at
        // compile-time that any new fields added there need to be considered here.
        let options = FindOptions {
            allow_disk_use: self.allow_disk_use,
            allow_partial_results: self.allow_partial_results,
            batch_size: self.batch_size,
            comment,
            comment_bson,
            hint: self.hint.clone(),
            limit: self.limit,
            max: self.max.clone(),
            max_scan: self.max_scan,
            max_time: self.max_time,
            min: self.min.clone(),
            no_cursor_timeout: self.no_cursor_timeout,
            projection: self.projection.clone(),
            read_concern: self.read_concern.clone(),
            return_key: self.return_key,
            show_record_id: self.show_record_id,
            skip: self.skip,
            sort: self.sort.clone(),
            collation: self.collation.clone(),
            cursor_type: None,
            max_await_time: None,
            selection_criteria: None,
            let_vars: self.let_vars.clone(),
        };
        match &self.session {
            Some(session_id) => {
                let cursor = with_mut_session!(test_runner, session_id, |session| async {
                    collection
                        .find_with_session(self.filter.clone(), options, session)
                        .await
                })
                .await?;
                Ok(TestCursor::Session {
                    cursor,
                    session_id: session_id.clone(),
                })
            }
            None => {
                let cursor = collection.find(self.filter.clone(), options).await?;
                Ok(TestCursor::Normal(Mutex::new(cursor)))
            }
        }
    }
}

impl TestOperation for Find {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match self.get_cursor(id, test_runner).await? {
                TestCursor::Session {
                    mut cursor,
                    session_id,
                } => {
                    with_mut_session!(test_runner, session_id.as_str(), |s| async {
                        cursor.stream(s).try_collect::<Vec<Document>>().await
                    })
                    .await?
                }
                TestCursor::Normal(cursor) => {
                    let cursor = cursor.into_inner();
                    cursor.try_collect::<Vec<Document>>().await?
                }
                TestCursor::ChangeStream(_) => panic!("get_cursor returned a change stream"),
                TestCursor::Closed => panic!("get_cursor returned a closed cursor"),
            };
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateFindCursor {
    // `Find` cannot be embedded directly because serde doesn't support combining `flatten`
    // and `deny_unknown_fields`, so its fields are replicated here.
    filter: Document,
    session: Option<String>,
    allow_disk_use: Option<bool>,
    allow_partial_results: Option<bool>,
    batch_size: Option<u32>,
    comment: Option<Bson>,
    hint: Option<Hint>,
    limit: Option<i64>,
    max: Option<Document>,
    max_scan: Option<u64>,
    #[serde(rename = "maxTimeMS")]
    max_time: Option<Duration>,
    min: Option<Document>,
    no_cursor_timeout: Option<bool>,
    projection: Option<Document>,
    read_concern: Option<ReadConcern>,
    return_key: Option<bool>,
    show_record_id: Option<bool>,
    skip: Option<u64>,
    sort: Option<Document>,
    collation: Option<Collation>,
    #[serde(rename = "let")]
    let_vars: Option<Document>,
}

impl TestOperation for CreateFindCursor {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let find = Find {
                filter: self.filter.clone(),
                session: self.session.clone(),
                allow_disk_use: self.allow_disk_use,
                allow_partial_results: self.allow_partial_results,
                batch_size: self.batch_size,
                comment: self.comment.clone(),
                hint: self.hint.clone(),
                limit: self.limit,
                max: self.max.clone(),
                max_scan: self.max_scan,
                max_time: self.max_time,
                min: self.min.clone(),
                no_cursor_timeout: self.no_cursor_timeout,
                projection: self.projection.clone(),
                read_concern: self.read_concern.clone(),
                return_key: self.return_key,
                show_record_id: self.show_record_id,
                skip: self.skip,
                sort: self.sort.clone(),
                collation: self.collation.clone(),
                let_vars: self.let_vars.clone(),
            };
            let cursor = find.get_cursor(id, test_runner).await?;
            Ok(Some(Entity::Cursor(cursor)))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        false
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertMany {
    documents: Vec<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: InsertManyOptions,
}

impl TestOperation for InsertMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| {
                        async move {
                            collection
                                .insert_many_with_session(
                                    self.documents.clone(),
                                    self.options.clone(),
                                    session,
                                )
                                .await
                        }
                        .boxed()
                    })
                    .await?
                }
                None => {
                    collection
                        .insert_many(self.documents.clone(), self.options.clone())
                        .await?
                }
            };
            let ids: HashMap<String, Bson> = result
                .inserted_ids
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            let ids = to_bson(&ids)?;
            Ok(Some(Bson::from(doc! { "insertedIds": ids }).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertOne {
    document: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: InsertOneOptions,
}

impl TestOperation for InsertOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        collection
                            .insert_one_with_session(
                                self.document.clone(),
                                self.options.clone(),
                                session,
                            )
                            .await
                    })
                    .await?
                }
                None => {
                    collection
                        .insert_one(self.document.clone(), self.options.clone())
                        .await?
                }
            };
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateMany {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: UpdateOptions,
}

impl TestOperation for UpdateMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .update_many(self.filter.clone(), self.update.clone())
                .with_options(self.options.clone())
                .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: UpdateOptions,
    session: Option<String>,
}

impl TestOperation for UpdateOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection
                .update_one(self.filter.clone(), self.update.clone())
                .with_options(self.options.clone());
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        act.session(session.deref_mut()).await
                    })
                    .await?
                }
                None => act.await?,
            };
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: AggregateOptions,
}

impl TestOperation for Aggregate {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match &self.session {
                Some(session_id) => {
                    enum AggregateEntity {
                        Collection(Collection<Document>),
                        Database(Database),
                        Other(String),
                    }
                    let entity = match test_runner.entities.read().await.get(id).unwrap() {
                        Entity::Collection(c) => AggregateEntity::Collection(c.clone()),
                        Entity::Database(d) => AggregateEntity::Database(d.clone()),
                        other => AggregateEntity::Other(format!("{:?}", other)),
                    };
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = match entity {
                            AggregateEntity::Collection(collection) => {
                                collection
                                    .aggregate(self.pipeline.clone())
                                    .with_options(self.options.clone())
                                    .session(session.deref_mut())
                                    .await?
                            }
                            AggregateEntity::Database(db) => {
                                db.aggregate(self.pipeline.clone())
                                    .with_options(self.options.clone())
                                    .session(session.deref_mut())
                                    .await?
                            }
                            AggregateEntity::Other(debug) => {
                                panic!("Cannot execute aggregate on {}", &debug)
                            }
                        };
                        cursor.stream(session).try_collect::<Vec<Document>>().await
                    })
                    .await?
                }
                None => {
                    let entities = test_runner.entities.read().await;
                    let cursor = match entities.get(id).unwrap() {
                        Entity::Collection(collection) => {
                            collection
                                .aggregate(self.pipeline.clone())
                                .with_options(self.options.clone())
                                .await?
                        }
                        Entity::Database(db) => {
                            db.aggregate(self.pipeline.clone())
                                .with_options(self.options.clone())
                                .await?
                        }
                        other => panic!("Cannot execute aggregate on {:?}", &other),
                    };
                    cursor.try_collect::<Vec<Document>>().await?
                }
            };
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Distinct {
    field_name: String,
    filter: Option<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: DistinctOptions,
}

impl TestOperation for Distinct {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection
                .distinct(&self.field_name, self.filter.clone().unwrap_or_default())
                .with_options(self.options.clone());
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        act.session(session.deref_mut()).await
                    })
                    .await?
                }
                None => act.await?,
            };
            Ok(Some(Bson::Array(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CountDocuments {
    session: Option<String>,
    filter: Option<Document>,
    #[serde(flatten)]
    options: CountOptions,
}

impl TestOperation for CountDocuments {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let action = collection
                .count_documents(self.filter.clone().unwrap_or_default())
                .with_options(self.options.clone());
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        action.session(session.deref_mut()).await
                    })
                    .await?
                }
                None => action.await?,
            };
            Ok(Some(Bson::Int64(result.try_into().unwrap()).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: EstimatedDocumentCountOptions,
}

impl TestOperation for EstimatedDocumentCount {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .estimated_document_count()
                .with_options(self.options.clone())
                .await?;
            Ok(Some(Bson::Int64(result.try_into().unwrap()).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Default)]
pub(super) struct FindOne {
    filter: Option<Document>,
    options: FindOneOptions,
}

// TODO RUST-1364: remove this impl and derive Deserialize instead
impl<'de> Deserialize<'de> for FindOne {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Helper {
            filter: Option<Document>,
            comment: Option<Bson>,
            #[serde(flatten)]
            options: FindOneOptions,
        }

        let mut helper = Helper::deserialize(deserializer)?;
        match helper.comment {
            Some(Bson::String(string)) => {
                helper.options.comment = Some(string);
            }
            Some(bson) => {
                helper.options.comment_bson = Some(bson);
            }
            _ => {}
        }

        Ok(Self {
            filter: helper.filter,
            options: helper.options,
        })
    }
}

impl TestOperation for FindOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .find_one(self.filter.clone(), self.options.clone())
                .await?;
            match result {
                Some(result) => Ok(Some(Bson::from(result).into())),
                None => Ok(Some(Entity::None)),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabases {
    session: Option<String>,
    #[serde(flatten)]
    options: crate::db::options::ListDatabasesOptions,
}

impl TestOperation for ListDatabases {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        let session: &mut crate::ClientSession = &mut *session;
                        client
                            .list_databases()
                            .with_options(self.options.clone())
                            .session(session)
                            .await
                    })
                    .await?
                }
                None => {
                    client
                        .list_databases()
                        .with_options(self.options.clone())
                        .await?
                }
            };
            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabaseNames {
    #[serde(flatten)]
    options: crate::db::options::ListDatabasesOptions,
}

impl TestOperation for ListDatabaseNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).await;
            let result = client
                .list_database_names()
                .with_options(self.options.clone())
                .await?;
            let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
            Ok(Some(Bson::Array(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollections {
    session: Option<String>,
    #[serde(flatten)]
    options: ListCollectionsOptions,
}

impl TestOperation for ListCollections {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = db
                            .list_collections()
                            .with_options(self.options.clone())
                            .session(session.deref_mut())
                            .await?;
                        cursor.stream(session).try_collect::<Vec<_>>().await
                    })
                    .await?
                }
                None => {
                    let cursor = db
                        .list_collections()
                        .with_options(self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<_>>().await?
                }
            };
            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

impl TestOperation for ListCollectionNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id).await;
            let result = db
                .list_collection_names()
                .optional(self.filter.clone(), |b, f| b.filter(f))
                .await?;
            let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
            Ok(Some(Bson::from(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: ReplaceOptions,
}

impl TestOperation for ReplaceOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .replace_one(
                    self.filter.clone(),
                    self.replacement.clone(),
                    self.options.clone(),
                )
                .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndUpdate {
    filter: Document,
    update: UpdateModifications,
    session: Option<String>,
    #[serde(flatten)]
    options: FindOneAndUpdateOptions,
}

impl TestOperation for FindOneAndUpdate {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        collection
                            .find_one_and_update_with_session(
                                self.filter.clone(),
                                self.update.clone(),
                                self.options.clone(),
                                session,
                            )
                            .await
                    })
                    .await?
                }
                None => {
                    collection
                        .find_one_and_update(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                        )
                        .await?
                }
            };
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndReplace {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: FindOneAndReplaceOptions,
}

impl TestOperation for FindOneAndReplace {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .find_one_and_replace(
                    self.filter.clone(),
                    self.replacement.clone(),
                    self.options.clone(),
                )
                .await?;
            let result = to_bson(&result)?;

            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndDelete {
    filter: Document,
    #[serde(flatten)]
    options: FindOneAndDeleteOptions,
}

impl TestOperation for FindOneAndDelete {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let result = collection
                .find_one_and_delete(self.filter.clone(), self.options.clone())
                .await?;
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FailPointCommand {
    fail_point: FailPoint,
    client: String,
}

impl TestOperation for FailPointCommand {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            let guard = self
                .fail_point
                .enable(&client, Some(ReadPreference::Primary.into()))
                .await
                .unwrap();
            test_runner.fail_point_guards.write().await.push(guard);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct TargetedFailPoint {
    fail_point: FailPoint,
    session: String,
}

impl TestOperation for TargetedFailPoint {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let selection_criteria =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    session
                        .transaction
                        .pinned_mongos()
                        .cloned()
                        .unwrap_or_else(|| panic!("ClientSession not pinned"))
                })
                .await;
            let fail_point_guard = test_runner
                .internal_client
                .enable_failpoint(self.fail_point.clone(), Some(selection_criteria))
                .await
                .unwrap();
            test_runner
                .fail_point_guards
                .write()
                .await
                .push(fail_point_guard);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionExists {
    collection_name: String,
    database_name: String,
}

impl TestOperation for AssertCollectionExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names().await.unwrap();
            assert!(names.contains(&self.collection_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionNotExists {
    collection_name: String,
    database_name: String,
}

impl TestOperation for AssertCollectionNotExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names().await.unwrap();
            assert!(!names.contains(&self.collection_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CreateCollection {
    collection: String,
    #[serde(flatten)]
    options: CreateCollectionOptions,
    session: Option<String>,
}

impl TestOperation for CreateCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.get_database(id).await;

            if let Some(session_id) = &self.session {
                with_mut_session!(test_runner, session_id, |session| async {
                    database
                        .create_collection(&self.collection)
                        .with_options(self.options.clone())
                        .session(session.deref_mut())
                        .await
                })
                .await?;
            } else {
                database
                    .create_collection(&self.collection)
                    .with_options(self.options.clone())
                    .await?;
            }
            Ok(Some(Entity::Collection(
                database.collection(&self.collection),
            )))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DropCollection {
    collection: String,
    #[serde(flatten)]
    options: DropCollectionOptions,
    session: Option<String>,
}

impl TestOperation for DropCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.get_database(id).await;
            let collection = database.collection::<Document>(&self.collection).clone();

            let act = collection.drop().with_options(self.options.clone());
            if let Some(session_id) = &self.session {
                with_mut_session!(test_runner, session_id, |session| async {
                    act.session(session.deref_mut()).await
                })
                .await?;
            } else {
                act.await?;
            }
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunCommand {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,
    read_preference: Option<SelectionCriteria>,
    session: Option<String>,
}

impl TestOperation for RunCommand {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();

            let db = test_runner.get_database(id).await;
            let action = db
                .run_command(command)
                .optional(self.read_preference.clone(), |a, rp| {
                    a.selection_criteria(rp)
                });
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        action.session(session.deref_mut()).await
                    })
                    .await?
                }
                None => action.await?,
            };
            let result = to_bson(&result)?;
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunCursorCommand {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,

    #[serde(flatten)]
    options: RunCursorCommandOptions,
    session: Option<String>,
}

impl TestOperation for RunCursorCommand {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();
            let db = test_runner.get_database(id).await;
            let options = self.options.clone();

            let action = db.run_cursor_command(command).with_options(options);
            let result = match &self.session {
                Some(session_id) => {
                    with_mut_session!(test_runner, session_id, |session| async {
                        let mut cursor = action.session(session.deref_mut()).await?;
                        cursor.stream(session).try_collect::<Vec<_>>().await
                    })
                    .await?
                }
                None => {
                    let cursor = action.await?;
                    cursor.try_collect::<Vec<_>>().await?
                }
            };

            Ok(Some(bson::to_bson(&result)?.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CreateCommandCursor {
    command: Document,
    // We don't need to use this field, but it needs to be included during deserialization so that
    // we can use the deny_unknown_fields tag.
    #[serde(rename = "commandName")]
    _command_name: String,

    #[serde(flatten)]
    options: RunCursorCommandOptions,
    session: Option<String>,
}

impl TestOperation for CreateCommandCursor {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let command = self.command.clone();
            let db = test_runner.get_database(id).await;
            let options = self.options.clone();

            let action = db.run_cursor_command(command).with_options(options);
            match &self.session {
                Some(session_id) => {
                    let mut ses_cursor = None;
                    with_mut_session!(test_runner, session_id, |session| async {
                        ses_cursor = Some(action.session(session.deref_mut()).await);
                    })
                    .await;
                    let test_cursor = TestCursor::Session {
                        cursor: ses_cursor.unwrap().unwrap(),
                        session_id: session_id.clone(),
                    };
                    Ok(Some(Entity::Cursor(test_cursor)))
                }
                None => {
                    let doc_cursor = action.await?;
                    let test_cursor = TestCursor::Normal(Mutex::new(doc_cursor));
                    Ok(Some(Entity::Cursor(test_cursor)))
                }
            }
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        false
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EndSession {}

impl TestOperation for EndSession {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| async {
                session.client_session.take();
            })
            .await;
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionTransactionState {
    session: String,
    state: String,
}

impl TestOperation for AssertSessionTransactionState {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session_state =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    match &session.transaction.state {
                        TransactionState::None => "none",
                        TransactionState::Starting => "starting",
                        TransactionState::InProgress => "inprogress",
                        TransactionState::Committed { data_committed: _ } => "committed",
                        TransactionState::Aborted => "aborted",
                    }
                })
                .await;
            assert_eq!(session_state, self.state);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionPinned {
    session: String,
}

impl TestOperation for AssertSessionPinned {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let is_pinned =
                with_mut_session!(test_runner, self.session.as_str(), |session| async {
                    session.transaction.pinned_mongos().is_some()
                })
                .await;
            assert!(is_pinned);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionUnpinned {
    session: String,
}

impl TestOperation for AssertSessionUnpinned {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let is_pinned = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.transaction.pinned_mongos().is_some() }
            })
            .await;
            assert!(!is_pinned);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertDifferentLsidOnLastTwoCommands {
    client: String,
}

impl TestOperation for AssertDifferentLsidOnLastTwoCommands {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let entities = test_runner.entities.read().await;
            let client = entities.get(&self.client).unwrap().as_client();
            let events = client.get_all_command_started_events();

            let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
            let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
            assert_ne!(lsid1, lsid2);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSameLsidOnLastTwoCommands {
    client: String,
}

impl TestOperation for AssertSameLsidOnLastTwoCommands {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let entities = test_runner.entities.read().await;
            let client = entities.get(&self.client).unwrap().as_client();
            client.sync_workers().await;
            let events = client.get_all_command_started_events();

            let lsid1 = events[events.len() - 1].command.get("lsid").unwrap();
            let lsid2 = events[events.len() - 2].command.get("lsid").unwrap();
            assert_eq!(lsid1, lsid2);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionDirty {
    session: String,
}

impl TestOperation for AssertSessionDirty {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let dirty = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.is_dirty() }.boxed()
            })
            .await;
            assert!(dirty);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertSessionNotDirty {
    session: String,
}

impl TestOperation for AssertSessionNotDirty {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let dirty = with_mut_session!(test_runner, self.session.as_str(), |session| {
                async move { session.is_dirty() }
            })
            .await;
            assert!(!dirty);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct StartTransaction {}

impl TestOperation for StartTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move { session.start_transaction(None).await }
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CommitTransaction {}

impl TestOperation for CommitTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move { session.commit_transaction().await }
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AbortTransaction {}

impl TestOperation for AbortTransaction {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            with_mut_session!(test_runner, id, |session| {
                async move { session.abort_transaction().await }
            })
            .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateIndex {
    session: Option<String>,
    keys: Document,
    name: Option<String>,
}

impl TestOperation for CreateIndex {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let options = IndexOptions::builder().name(self.name.clone()).build();
            let index = IndexModel::builder()
                .keys(self.keys.clone())
                .options(options)
                .build();

            let collection = test_runner.get_collection(id).await;
            let act = collection.create_index(index);
            let name = match self.session {
                Some(ref session_id) => {
                    with_mut_session!(test_runner, session_id, |session| {
                        async move {
                            act.session(session.deref_mut())
                                .await
                                .map(|model| model.index_name)
                        }
                    })
                    .await?
                }
                None => act.await?.index_name,
            };
            Ok(Some(Bson::String(name).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexes {
    session: Option<String>,
    #[serde(flatten)]
    options: ListIndexesOptions,
}

impl TestOperation for ListIndexes {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection.list_indexes().with_options(self.options.clone());
            let indexes: Vec<IndexModel> = match self.session {
                Some(ref session) => {
                    with_mut_session!(test_runner, session, |session| {
                        async {
                            act.session(session.deref_mut())
                                .await?
                                .stream(session)
                                .try_collect()
                                .await
                        }
                    })
                    .await?
                }
                None => act.await?.try_collect().await?,
            };
            let indexes: Vec<Document> = indexes
                .iter()
                .map(|index| bson::to_document(index).unwrap())
                .collect();
            Ok(Some(Bson::from(indexes).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexNames {
    session: Option<String>,
}

impl TestOperation for ListIndexNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).await;
            let act = collection.list_index_names();
            let names = match self.session {
                Some(ref session) => {
                    with_mut_session!(test_runner, session.as_str(), |s| {
                        async move { act.session(s.deref_mut()).await }
                    })
                    .await?
                }
                None => act.await?,
            };
            Ok(Some(Bson::from(names).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertIndexExists {
    collection_name: String,
    database_name: String,
    index_name: String,
}

impl TestOperation for AssertIndexExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let coll = test_runner
                .internal_client
                .database(&self.database_name)
                .collection::<Document>(&self.collection_name);
            let names = coll.list_index_names().await.unwrap();
            assert!(names.contains(&self.index_name));
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertIndexNotExists {
    collection_name: String,
    database_name: String,
    index_name: String,
}

impl TestOperation for AssertIndexNotExists {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let coll = test_runner
                .internal_client
                .database(&self.database_name)
                .collection::<Document>(&self.collection_name);
            match coll.list_index_names().await {
                Ok(indexes) => assert!(!indexes.contains(&self.index_name)),
                // a namespace not found error indicates that the index does not exist
                Err(err) => assert_eq!(err.sdam_code(), Some(26)),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct IterateUntilDocumentOrError {}

impl TestOperation for IterateUntilDocumentOrError {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            // A `SessionCursor` also requires a `&mut Session`, which would cause conflicting
            // borrows, so take the cursor from the map and return it after execution instead.
            let mut cursor = test_runner.take_cursor(id).await;
            let next = match &mut cursor {
                TestCursor::Normal(cursor) => {
                    let mut cursor = cursor.lock().await;
                    cursor.next().await
                }
                TestCursor::Session { cursor, session_id } => {
                    cursor
                        .next(
                            test_runner
                                .entities
                                .write()
                                .await
                                .get_mut(session_id)
                                .unwrap()
                                .as_mut_session_entity(),
                        )
                        .await
                }
                TestCursor::ChangeStream(stream) => {
                    let mut stream = stream.lock().await;
                    stream.next().await.map(|res| {
                        res.map(|ev| match bson::to_bson(&ev) {
                            Ok(Bson::Document(doc)) => doc,
                            _ => panic!("invalid serialization result"),
                        })
                    })
                }
                TestCursor::Closed => None,
            };
            test_runner.return_cursor(id, cursor).await;
            next.transpose()
                .map(|opt| opt.map(|doc| Entity::Bson(Bson::Document(doc))))
        }
        .boxed()
    }

    fn returns_root_documents(&self) -> bool {
        true
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Close {}

impl TestOperation for Close {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let mut entities = test_runner.entities.write().await;
            let target_entity = entities.get(id).unwrap();
            match target_entity {
                Entity::Client(_) => {
                    let client = entities.get_mut(id).unwrap().as_mut_client();
                    let closed_client_topology_id = client.topology_id;
                    client.client = None;

                    let mut entities_to_remove = vec![];
                    for (key, value) in entities.iter() {
                        match value {
                            // skip clients so that we don't remove the client entity itself from
                            // the map: we want to preserve it so we can
                            // access the other data stored on the entity.
                            Entity::Client(_) => {}
                            _ => {
                                if value.client_topology_id().await
                                    == Some(closed_client_topology_id)
                                {
                                    entities_to_remove.push(key.clone());
                                }
                            }
                        }
                    }
                    for entity_id in entities_to_remove {
                        entities.remove(&entity_id);
                    }

                    Ok(None)
                }
                Entity::Cursor(_) => {
                    let cursor = entities.get_mut(id).unwrap().as_mut_cursor();
                    let rx = cursor.make_kill_watcher().await;
                    *cursor = TestCursor::Closed;
                    drop(entities);
                    let _ = rx.await;
                    Ok(None)
                }
                _ => panic!(
                    "Unsupported entity {:?} for close operation; expected Client or Cursor",
                    target_entity
                ),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertNumberConnectionsCheckedOut {
    client: String,
    connections: u32,
}

impl TestOperation for AssertNumberConnectionsCheckedOut {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            client.sync_workers().await;
            assert_eq!(client.connections_checked_out(), self.connections);
        }
        .boxed()
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RenameCollection {
    to: String,
}

impl TestOperation for RenameCollection {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let target = test_runner.get_collection(id).await;
            let ns = target.namespace();
            let mut to_ns = ns.clone();
            to_ns.coll = self.to.clone();
            let cmd = doc! {
                "renameCollection": crate::bson::to_bson(&ns)?,
                "to": crate::bson::to_bson(&to_ns)?,
            };
            let admin = test_runner.internal_client.database("admin");
            admin.run_command(cmd).await?;
            Ok(None)
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
                            match expected_error.verify_result(&error, operation.name.as_str()) {
                                Ok(_) => self.report_success(&mut entities),
                                Err(e) => report_error_or_failure!(
                                    self.store_failures_as_entity,
                                    self.store_errors_as_entity,
                                    e,
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
pub(super) struct RunOnThread {
    thread: String,
    operation: Arc<Operation>,
}

impl TestOperation for RunOnThread {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let thread = test_runner.get_thread(self.thread.as_str()).await;
            thread.run_operation(self.operation.clone());
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForThread {
    thread: String,
}

impl TestOperation for WaitForThread {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let thread = test_runner.get_thread(self.thread.as_str()).await;
            thread.wait().await.unwrap_or_else(|e| {
                panic!("thread {:?} did not exit successfully: {}", self.thread, e)
            });
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertEventCount {
    client: String,
    event: ExpectedEvent,
    count: usize,
}

impl TestOperation for AssertEventCount {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(self.client.as_str()).await;
            let entities = test_runner.entities.clone();
            let actual_events = client
                .observer
                .lock()
                .await
                .matching_events(&self.event, entities)
                .await;
            assert_eq!(
                actual_events.len(),
                self.count,
                "expected to see {} events matching: {:#?}, instead saw: {:#?}",
                self.count,
                self.event,
                actual_events
            );
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForEvent {
    client: String,
    event: ExpectedEvent,
    count: usize,
}

impl TestOperation for WaitForEvent {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(self.client.as_str()).await;
            let entities = test_runner.entities.clone();
            client
                .observer
                .lock()
                .await
                .wait_for_matching_events(&self.event, self.count, entities)
                .await
                .unwrap();
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RecordTopologyDescription {
    id: String,
    client: String,
}

impl TestOperation for RecordTopologyDescription {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let client = test_runner.get_client(&self.client).await;
            let description = client.topology_description();
            test_runner.insert_entity(&self.id, description).await;
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertTopologyType {
    topology_description: String,
    topology_type: TopologyType,
}

impl TestOperation for AssertTopologyType {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async {
            let td = test_runner
                .get_topology_description(&self.topology_description)
                .await;
            assert_eq!(td.topology_type, self.topology_type);
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WaitForPrimaryChange {
    client: String,
    prior_topology_description: String,
    #[serde(rename = "timeoutMS")]
    timeout_ms: Option<u64>,
}

impl TestOperation for WaitForPrimaryChange {
    fn execute_test_runner_operation<'a>(
        &'a self,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client).await;
            let td = test_runner
                .get_topology_description(&self.prior_topology_description)
                .await;
            let old_primary = td.servers_with_type(&[ServerType::RsPrimary]).next();
            let timeout = Duration::from_millis(self.timeout_ms.unwrap_or(10_000));

            runtime::timeout(timeout, async {
                let mut watcher = client.topology().watch();

                loop {
                    let latest = watcher.observe_latest();
                    if let Some(primary) = latest.description.primary() {
                        if Some(primary) != old_primary {
                            return;
                        }
                    }
                    watcher.wait_for_update(None).await;
                }
            })
            .await
            .unwrap();
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Wait {
    ms: u64,
}

impl TestOperation for Wait {
    fn execute_test_runner_operation<'a>(
        &'a self,
        _test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, ()> {
        tokio::time::sleep(Duration::from_millis(self.ms)).boxed()
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

    fn test_file_entities(&self) -> Option<&Vec<TestFileEntity>> {
        Some(&self.entities)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Download {
    id: Bson,
}

impl TestOperation for Download {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;

            // First, read via the download_to_writer API.
            let mut buf: Vec<u8> = vec![];
            bucket
                .download_to_futures_0_3_writer(self.id.clone(), &mut buf)
                .await?;
            let writer_data = hex::encode(buf);

            // Next, read via the open_download_stream API.
            let mut buf: Vec<u8> = vec![];
            let mut stream = bucket.open_download_stream(self.id.clone()).await?;
            stream.read_to_end(&mut buf).await?;
            let stream_data = hex::encode(buf);

            // Assert that both APIs returned the same data.
            assert_eq!(writer_data, stream_data);

            Ok(Some(Entity::Bson(writer_data.into())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DownloadByName {
    filename: String,
    #[serde(flatten)]
    options: GridFsDownloadByNameOptions,
}

impl TestOperation for DownloadByName {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;

            // First, read via the download_to_writer API.
            let mut buf: Vec<u8> = vec![];
            bucket
                .download_to_futures_0_3_writer_by_name(
                    self.filename.clone(),
                    &mut buf,
                    self.options.clone(),
                )
                .await?;
            let writer_data = hex::encode(buf);

            // Next, read via the open_download_stream API.
            let mut buf: Vec<u8> = vec![];
            let mut stream = bucket
                .open_download_stream_by_name(self.filename.clone(), self.options.clone())
                .await?;
            stream.read_to_end(&mut buf).await?;
            let stream_data = hex::encode(buf);

            // Assert that both APIs returned the same data.
            assert_eq!(writer_data, stream_data);

            Ok(Some(Entity::Bson(writer_data.into())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Delete {
    id: Bson,
}

impl TestOperation for Delete {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;
            bucket.delete(self.id.clone()).await?;
            Ok(None)
        }
        .boxed()
    }
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Upload {
    source: Document,
    filename: String,
    // content_type and disableMD5 are deprecated and no longer supported.
    // Options included for deserialization.
    #[serde(rename = "contentType")]
    _content_type: Option<String>,
    #[serde(rename = "disableMD5")]
    _disable_md5: Option<bool>,
    #[serde(flatten)]
    options: GridFsUploadOptions,
}

impl TestOperation for Upload {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let bucket = test_runner.get_bucket(id).await;
            let hex_string = self.source.get("$$hexBytes").unwrap().as_str().unwrap();
            let bytes = hex::decode(hex_string).unwrap();

            let id = bucket
                .upload_from_futures_0_3_reader(
                    self.filename.clone(),
                    &bytes[..],
                    self.options.clone(),
                )
                .await?;

            Ok(Some(Entity::Bson(id.into())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct IterateOnce {}

impl TestOperation for IterateOnce {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let mut cursor = test_runner.take_cursor(id).await;
            match &mut cursor {
                TestCursor::Normal(cursor) => {
                    let mut cursor = cursor.lock().await;
                    cursor.try_advance().await?;
                }
                TestCursor::Session { cursor, session_id } => {
                    cursor
                        .try_advance(
                            test_runner
                                .entities
                                .write()
                                .await
                                .get_mut(session_id)
                                .unwrap()
                                .as_mut_session_entity(),
                        )
                        .await?;
                }
                TestCursor::ChangeStream(change_stream) => {
                    let mut change_stream = change_stream.lock().await;
                    change_stream.next_if_any().await?;
                }
                TestCursor::Closed => panic!("Attempted to call IterateOnce on a closed cursor"),
            }
            test_runner.return_cursor(id, cursor).await;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation {
    _name: String,
}

impl TestOperation for UnimplementedOperation {}
