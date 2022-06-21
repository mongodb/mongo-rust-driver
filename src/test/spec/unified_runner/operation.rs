use std::{collections::HashMap, convert::TryInto, fmt::Debug, ops::Deref, time::Duration};

use futures::{
    future::BoxFuture,
    stream::{StreamExt, TryStreamExt},
    FutureExt,
};
use serde::{de::Deserializer, Deserialize};
use tokio::sync::Mutex;

use super::{Entity, ExpectError, TestCursor, TestRunner};

use crate::{
    bson::{doc, to_bson, Bson, Deserializer as BsonDeserializer, Document},
    bson_util,
    change_stream::options::ChangeStreamOptions,
    client::session::{ClientSession, TransactionState},
    coll::options::Hint,
    collation::Collation,
    error::Result,
    options::{
        AggregateOptions,
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
        ListCollectionsOptions,
        ListDatabasesOptions,
        ListIndexesOptions,
        ReadConcern,
        ReplaceOptions,
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
    },
    runtime,
    selection_criteria::ReadPreference,
    test::FailPoint,
    Collection,
    Database,
    IndexModel,
};

pub trait TestOperation: Debug {
    fn execute_test_runner_operation<'a>(
        &'a self,
        _test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        todo!()
    }

    fn execute_entity_operation<'a>(
        &'a self,
        _id: &'a str,
        _test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        todo!()
    }

    /// Whether or not this operation returns an array of root documents. This information is
    /// necessary to determine how the return value of an operation should be compared to the
    /// expected value.
    fn returns_root_documents(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct Operation {
    operation: Box<dyn TestOperation>,
    pub name: String,
    pub object: OperationObject,
    pub expectation: Expectation,
}

#[derive(Debug)]
pub enum OperationObject {
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
pub enum Expectation {
    Result {
        expected_value: Option<Bson>,
        save_as_entity: Option<String>,
    },
    Error(ExpectError),
    Ignore,
}

fn deserialize_op<'de, 'a, T: 'a + Deserialize<'de> + TestOperation>(
    value: Bson,
) -> std::result::Result<Box<dyn TestOperation + 'a>, bson::de::Error> {
    T::deserialize(BsonDeserializer::new(value)).map(|op| Box::new(op) as Box<dyn TestOperation>)
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OperationDefinition {
            pub name: String,
            pub object: OperationObject,
            #[serde(default = "default_arguments")]
            pub arguments: Bson,
            pub expect_error: Option<ExpectError>,
            pub expect_result: Option<Bson>,
            pub save_result_as_entity: Option<String>,
            pub ignore_result_and_error: Option<bool>,
        }

        fn default_arguments() -> Bson {
            Bson::Document(doc! {})
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
            _ => Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>),
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
    // TODO: RUST-1071 add comment to DeleteOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for DeleteMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
            let result = collection
                .delete_many(self.filter.clone(), self.options.clone())
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
    // TODO: RUST-1071 add comment to DeleteOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for DeleteOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    collection
                        .delete_one_with_session(self.filter.clone(), self.options.clone(), session)
                        .await?
                }
                None => {
                    collection
                        .delete_one(self.filter.clone(), self.options.clone())
                        .await?
                }
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
        deserialize_with = "bson_util::deserialize_duration_option_from_u64_millis"
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
        test_runner: &'a mut TestRunner,
    ) -> Result<TestCursor> {
        let collection = test_runner.get_collection(id).clone();
        // `FindOptions` is constructed without the use of `..Default::default()` to enforce at
        // compile-time that any new fields added there need to be considered here.
        let comment = if let Some(Bson::String(s)) = &self.comment {
            Some(s.clone())
        } else {
            None
        };
        let options = FindOptions {
            allow_disk_use: self.allow_disk_use,
            allow_partial_results: self.allow_partial_results,
            batch_size: self.batch_size,
            comment,
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
                let session = test_runner.get_mut_session(session_id);
                let cursor = collection
                    .find_with_session(self.filter.clone(), options, session)
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match self.get_cursor(id, test_runner).await? {
                TestCursor::Session {
                    mut cursor,
                    session_id,
                } => {
                    let session = test_runner.get_mut_session(&session_id);
                    cursor
                        .stream(session)
                        .try_collect::<Vec<Document>>()
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
        false
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
        test_runner: &'a mut TestRunner,
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
    // TODO: RUST-1071 add comment to InsertManyOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for InsertMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    collection
                        .insert_many_with_session(
                            self.documents.clone(),
                            self.options.clone(),
                            session,
                        )
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
    // TODO: RUST-1071 add comment to InsertOneOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
    #[serde(flatten)]
    options: InsertOneOptions,
}

impl TestOperation for InsertOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    collection
                        .insert_one_with_session(
                            self.document.clone(),
                            self.options.clone(),
                            test_runner.get_mut_session(session_id),
                        )
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
    // TODO: RUST-1071 add comment to UpdateOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for UpdateMany {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
            let result = collection
                .update_many(
                    self.filter.clone(),
                    self.update.clone(),
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
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: UpdateOptions,
    session: Option<String>,
    // TODO: RUST-1071 add comment to UpdateOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for UpdateOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    collection
                        .update_one_with_session(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                            test_runner.get_mut_session(session_id),
                        )
                        .await?
                }
                None => {
                    collection
                        .update_one(
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
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: AggregateOptions,
    // TODO: RUST-1071 add comment to AggregateOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for Aggregate {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let result = match &self.session {
                Some(session_id) => {
                    enum AggregateEntity {
                        Collection(Collection<Document>),
                        Database(Database),
                        Other(String),
                    }
                    let entity = match test_runner.entities.get(id).unwrap() {
                        Entity::Collection(c) => AggregateEntity::Collection(c.clone()),
                        Entity::Database(d) => AggregateEntity::Database(d.clone()),
                        other => AggregateEntity::Other(format!("{:?}", other)),
                    };
                    let session = test_runner.get_mut_session(session_id);
                    let mut cursor = match entity {
                        AggregateEntity::Collection(collection) => {
                            collection
                                .aggregate_with_session(
                                    self.pipeline.clone(),
                                    self.options.clone(),
                                    session,
                                )
                                .await?
                        }
                        AggregateEntity::Database(db) => {
                            db.aggregate_with_session(
                                self.pipeline.clone(),
                                self.options.clone(),
                                session,
                            )
                            .await?
                        }
                        AggregateEntity::Other(debug) => {
                            panic!("Cannot execute aggregate on {}", &debug)
                        }
                    };
                    cursor
                        .stream(session)
                        .try_collect::<Vec<Document>>()
                        .await?
                }
                None => {
                    let cursor = match test_runner.entities.get(id).unwrap() {
                        Entity::Collection(collection) => {
                            collection
                                .aggregate(self.pipeline.clone(), self.options.clone())
                                .await?
                        }
                        Entity::Database(db) => {
                            db.aggregate(self.pipeline.clone(), self.options.clone())
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    collection
                        .distinct_with_session(
                            &self.field_name,
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .distinct(&self.field_name, self.filter.clone(), self.options.clone())
                        .await?
                }
            };
            Ok(Some(Bson::Array(result).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CountDocuments {
    filter: Document,
    session: Option<String>,
    // TODO: RUST-1071 add comment to CountOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
    #[serde(flatten)]
    options: CountOptions,
}

impl TestOperation for CountDocuments {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    collection
                        .count_documents_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .count_documents(self.filter.clone(), self.options.clone())
                        .await?
                }
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
            let result = collection
                .estimated_document_count(self.options.clone())
                .await?;
            Ok(Some(Bson::Int64(result.try_into().unwrap()).into()))
        }
        .boxed()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOne {
    filter: Option<Document>,
    #[serde(flatten)]
    options: FindOneOptions,
}

impl TestOperation for FindOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
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
    filter: Option<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: ListDatabasesOptions,
}

impl TestOperation for ListDatabases {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    client
                        .list_databases_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    client
                        .list_databases(self.filter.clone(), self.options.clone())
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
    filter: Option<Document>,
    #[serde(flatten)]
    options: ListDatabasesOptions,
}

impl TestOperation for ListDatabaseNames {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let client = test_runner.get_client(id);
            let result = client
                .list_database_names(self.filter.clone(), self.options.clone())
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
    filter: Option<Document>,
    session: Option<String>,
    #[serde(flatten)]
    options: ListCollectionsOptions,
}

impl TestOperation for ListCollections {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    let mut cursor = db
                        .list_collections_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?;
                    cursor.stream(session).try_collect::<Vec<_>>().await?
                }
                None => {
                    let cursor = db
                        .list_collections(self.filter.clone(), self.options.clone())
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let db = test_runner.get_database(id);
            let result = db.list_collection_names(self.filter.clone()).await?;
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
    // TODO: RUST-1071 add comment to ReplaceOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for ReplaceOne {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
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
    // TODO: RUST-1071 add comment to FindOneAndUpdateOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for FindOneAndUpdate {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    collection
                        .find_one_and_update_with_session(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                            session,
                        )
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
    // TODO: RUST-1071 add comment to FindOneandReplaceOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for FindOneAndReplace {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
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
    // TODO: RUST-1071 add comment to FindOneAndDeleteOptions.
    #[serde(rename = "comment")]
    _comment: Option<Bson>,
}

impl TestOperation for FindOneAndDelete {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id);
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client);
            let guard = self
                .fail_point
                .clone()
                .enable(client, Some(ReadPreference::Primary.into()))
                .await
                .unwrap();
            test_runner.fail_point_guards.push(guard);
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session = test_runner.get_session(&self.session);
            let selection_criteria = session
                .transaction
                .pinned_mongos()
                .cloned()
                .unwrap_or_else(|| panic!("ClientSession not pinned"));
            let fail_point_guard = test_runner
                .internal_client
                .enable_failpoint(self.fail_point.clone(), Some(selection_criteria))
                .await
                .unwrap();
            test_runner.fail_point_guards.push(fail_point_guard);
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names(None).await.unwrap();
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let db = test_runner.internal_client.database(&self.database_name);
            let names = db.list_collection_names(None).await.unwrap();
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.get_database(id).clone();

            if let Some(session_id) = &self.session {
                database
                    .create_collection_with_session(
                        &self.collection,
                        self.options.clone(),
                        test_runner.get_mut_session(session_id),
                    )
                    .await?;
            } else {
                database
                    .create_collection(&self.collection, self.options.clone())
                    .await?;
            }
            Ok(None)
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let database = test_runner.entities.get(id).unwrap().as_database();
            let collection = database.collection::<Document>(&self.collection).clone();

            if let Some(session_id) = &self.session {
                collection
                    .drop_with_session(
                        self.options.clone(),
                        test_runner.get_mut_session(session_id),
                    )
                    .await?;
            } else {
                collection.drop(self.options.clone()).await?;
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
    read_concern: Option<Document>,
    read_preference: Option<SelectionCriteria>,
    session: Option<String>,
    write_concern: Option<Document>,
}

impl TestOperation for RunCommand {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let mut command = self.command.clone();
            if let Some(ref read_concern) = self.read_concern {
                command.insert("readConcern", read_concern.clone());
            }
            if let Some(ref write_concern) = self.write_concern {
                command.insert("writeConcern", write_concern.clone());
            }

            let db = test_runner.get_database(id).clone();
            let result = match &self.session {
                Some(session_id) => {
                    let session = test_runner.get_mut_session(session_id);
                    db.run_command_with_session(command, self.read_preference.clone(), session)
                        .await?
                }
                None => {
                    db.run_command(command, self.read_preference.clone())
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
pub(super) struct EndSession {}

impl TestOperation for EndSession {
    fn execute_entity_operation<'a>(
        &'a self,
        id: &'a str,
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let session = test_runner.get_mut_session(id).client_session.take();
            drop(session);
            runtime::delay_for(Duration::from_secs(1)).await;
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session: &ClientSession = test_runner.get_session(&self.session);
            let session_state = match &session.transaction.state {
                TransactionState::None => "none",
                TransactionState::Starting => "starting",
                TransactionState::InProgress => "inprogress",
                TransactionState::Committed { data_committed: _ } => "committed",
                TransactionState::Aborted => "aborted",
            };
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            assert!(test_runner
                .get_session(&self.session)
                .transaction
                .pinned_mongos()
                .is_some());
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            assert!(test_runner
                .get_session(&self.session)
                .transaction
                .pinned_mongos()
                .is_none());
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.entities.get(&self.client).unwrap().as_client();
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.entities.get(&self.client).unwrap().as_client();
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session: &ClientSession = test_runner.get_session(&self.session);
            assert!(session.is_dirty());
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let session: &ClientSession = test_runner.get_session(&self.session);
            assert!(!session.is_dirty());
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let session: &mut ClientSession = test_runner.get_mut_session(id);
            session.start_transaction(None).await?;
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let session: &mut ClientSession = test_runner.get_mut_session(id);
            session.commit_transaction().await?;
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let session: &mut ClientSession = test_runner.get_mut_session(id);
            session.abort_transaction().await?;
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let options = IndexOptions::builder().name(self.name.clone()).build();
            let index = IndexModel::builder()
                .keys(self.keys.clone())
                .options(options)
                .build();

            let collection = test_runner.get_collection(id).clone();
            let name = match self.session {
                Some(ref session) => {
                    let session = test_runner.get_mut_session(session);
                    collection
                        .create_index_with_session(index, None, session)
                        .await?
                        .index_name
                }
                None => collection.create_index(index, None).await?.index_name,
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let indexes: Vec<IndexModel> = match self.session {
                Some(ref session) => {
                    let session = test_runner.get_mut_session(session);
                    collection
                        .list_indexes_with_session(self.options.clone(), session)
                        .await?
                        .stream(session)
                        .try_collect()
                        .await?
                }
                None => {
                    collection
                        .list_indexes(self.options.clone())
                        .await?
                        .try_collect()
                        .await?
                }
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let collection = test_runner.get_collection(id).clone();
            let names = match self.session {
                Some(ref session) => {
                    let session = test_runner.get_mut_session(session);
                    collection.list_index_names_with_session(session).await?
                }
                None => collection.list_index_names().await?,
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
        test_runner: &'a mut TestRunner,
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let coll = test_runner
                .internal_client
                .database(&self.database_name)
                .collection::<Document>(&self.collection_name);
            match coll.list_index_names().await {
                Ok(indexes) => assert!(!indexes.contains(&self.index_name)),
                // a namespace not found error indicates that the index does not exist
                Err(err) => assert_eq!(err.code(), Some(26)),
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            // A `SessionCursor` also requires a `&mut Session`, which would cause conflicting
            // borrows, so take the cursor from the map and return it after execution instead.
            let mut cursor = test_runner.entities.remove(id).unwrap().into_cursor();
            let next = match &mut cursor {
                TestCursor::Normal(cursor) => {
                    let mut cursor = cursor.lock().await;
                    cursor.next().await
                }
                TestCursor::Session { cursor, session_id } => {
                    let session = test_runner.get_mut_session(session_id);
                    cursor.next(session).await
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
            test_runner
                .entities
                .insert(id.to_string(), Entity::Cursor(cursor));
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let cursor = test_runner.get_mut_find_cursor(id);
            let rx = cursor.make_kill_watcher().await;
            *cursor = TestCursor::Closed;
            let _ = rx.await;
            Ok(None)
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, ()> {
        async move {
            let client = test_runner.get_client(&self.client);
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let target = test_runner.entities.get(id).unwrap();
            let stream = match target {
                Entity::Client(ce) => {
                    ce.watch(self.pipeline.clone(), self.options.clone())
                        .await?
                }
                Entity::Database(db) => {
                    db.watch(self.pipeline.clone(), self.options.clone())
                        .await?
                }
                Entity::Collection(coll) => {
                    coll.watch(self.pipeline.clone(), self.options.clone())
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
        test_runner: &'a mut TestRunner,
    ) -> BoxFuture<'a, Result<Option<Entity>>> {
        async move {
            let target = test_runner.get_collection(id);
            let ns = target.namespace();
            let mut to_ns = ns.clone();
            to_ns.coll = self.to.clone();
            let cmd = doc! {
                "renameCollection": crate::bson::to_bson(&ns)?,
                "to": crate::bson::to_bson(&to_ns)?,
            };
            let admin = test_runner.internal_client.database("admin");
            admin.run_command(cmd, None).await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

impl TestOperation for UnimplementedOperation {}
