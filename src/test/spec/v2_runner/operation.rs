use std::{collections::HashMap, convert::TryInto, fmt::Debug, ops::Deref};

use futures::{future::BoxFuture, stream::TryStreamExt, FutureExt};
use serde::{de::Deserializer, Deserialize};

use crate::{
    bson::{doc, to_bson, Bson, Deserializer as BsonDeserializer, Document},
    client::session::TransactionState,
    db::options::ListCollectionsOptions,
    error::Result,
    options::{
        AggregateOptions,
        CollectionOptions,
        CountOptions,
        CreateCollectionOptions,
        DatabaseOptions,
        DeleteOptions,
        DistinctOptions,
        DropCollectionOptions,
        DropIndexOptions,
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
        TransactionOptions,
        UpdateModifications,
        UpdateOptions,
    },
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::{assert_matches, log_uncaptured, FailPoint, TestClient},
    ClientSession,
    Collection,
    Database,
    IndexModel,
    OptionalUpdate,
};

use super::{OpRunner, OpSessions};

pub(crate) trait TestOperation: Debug + Send + Sync {
    fn execute_on_collection<'a>(
        &'a self,
        _collection: &'a Collection<Document>,
        _session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        todo!()
    }

    fn execute_on_database<'a>(
        &'a self,
        _database: &'a Database,
        _session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        todo!()
    }

    fn execute_on_client<'a>(
        &'a self,
        _client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        todo!()
    }

    fn execute_on_session<'a>(
        &'a self,
        _session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        todo!()
    }

    fn execute_recursive<'a>(
        &'a self,
        _runner: &'a mut OpRunner,
        _sessions: OpSessions<'a>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        todo!()
    }
}

#[derive(Debug)]
pub(crate) struct Operation {
    operation: Box<dyn TestOperation>,
    pub(crate) name: String,
    pub(crate) object: Option<OperationObject>,
    pub(crate) collection_options: Option<CollectionOptions>,
    pub(crate) database_options: Option<DatabaseOptions>,
    pub(crate) error: Option<bool>,
    pub(crate) result: Option<OperationResult>,
    pub(crate) session: Option<String>,
}

impl Operation {
    pub(crate) fn assert_result_matches(&self, result: &Result<Option<Bson>>, description: &str) {
        if self.error.is_none() && self.result.is_none() && result.is_err() {
            log_uncaptured(format!(
                "Ignoring operation error: {}",
                result.clone().unwrap_err()
            ));
        }

        if let Some(error) = self.error {
            assert_eq!(error, result.is_err(), "{}", description);
        }

        if let Some(expected_result) = &self.result {
            match expected_result {
                OperationResult::Success(expected) => {
                    let result = match result.as_ref() {
                        Ok(Some(r)) => r,
                        _ => panic!("{}: expected value, got {:?}", description, result),
                    };
                    assert_matches(result, expected, Some(description));
                }
                OperationResult::Error(operation_error) => {
                    assert!(
                        result.is_err(),
                        "{}: expected error\n{:#?}  got value\n{:#?}",
                        description,
                        operation_error,
                        result,
                    );
                    let error = result.as_ref().unwrap_err();
                    if let Some(error_contains) = &operation_error.error_contains {
                        let message = error.message().unwrap().to_lowercase();
                        assert!(
                            message.contains(&error_contains.to_lowercase()),
                            "{}: expected error message to contain \"{}\" but got \"{}\"",
                            description,
                            error_contains,
                            message
                        );
                    }
                    if let Some(error_code_name) = &operation_error.error_code_name {
                        let code_name = error.code_name().unwrap();
                        assert_eq!(
                            error_code_name, code_name,
                            "{}: expected error with codeName {:?}, instead got {:#?}",
                            description, error_code_name, error
                        );
                    }
                    if let Some(error_code) = operation_error.error_code {
                        let code = error.sdam_code().unwrap();
                        assert_eq!(error_code, code);
                    }
                    if let Some(error_labels_contain) = &operation_error.error_labels_contain {
                        let labels = error.labels();
                        error_labels_contain
                            .iter()
                            .for_each(|label| assert!(labels.contains(label)));
                    }
                    if let Some(error_labels_omit) = &operation_error.error_labels_omit {
                        let labels = error.labels();
                        error_labels_omit
                            .iter()
                            .for_each(|label| assert!(!labels.contains(label)));
                    }
                    #[cfg(feature = "in-use-encryption-unstable")]
                    if let Some(t) = &operation_error.is_timeout_error {
                        assert_eq!(
                            *t,
                            error.is_network_timeout() || error.is_non_timeout_network_error()
                        )
                    }
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum OperationObject {
    Database,
    Collection,
    Client,
    Session0,
    Session1,
    #[serde(rename = "gridfsbucket")]
    GridfsBucket,
    TestRunner,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum OperationResult {
    Error(OperationError),
    Success(Bson),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct OperationError {
    pub(crate) error_contains: Option<String>,
    pub(crate) error_code_name: Option<String>,
    pub(crate) error_code: Option<i32>,
    pub(crate) error_labels_contain: Option<Vec<String>>,
    pub(crate) error_labels_omit: Option<Vec<String>>,
    #[cfg(feature = "in-use-encryption-unstable")]
    pub(crate) is_timeout_error: Option<bool>,
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OperationDefinition {
            pub(crate) name: String,
            pub(crate) object: Option<OperationObject>,
            pub(crate) collection_options: Option<CollectionOptions>,
            pub(crate) database_options: Option<DatabaseOptions>,
            #[serde(default = "default_arguments")]
            pub(crate) arguments: Document,
            pub(crate) error: Option<bool>,
            pub(crate) result: Option<OperationResult>,
            // We don't need to use this field, but it needs to be included during deserialization
            // so that we can use the deny_unknown_fields tag.
            #[serde(rename = "command_name")]
            pub(crate) _command_name: Option<String>,
        }

        fn default_arguments() -> Document {
            doc! {}
        }

        let mut definition = OperationDefinition::deserialize(deserializer)?;
        let session = definition
            .arguments
            .remove("session")
            .map(|session| session.as_str().unwrap().to_string());

        let boxed_op = match definition.name.as_str() {
            "insertOne" => deserialize_op::<InsertOne>(definition.arguments),
            "insertMany" => deserialize_op::<InsertMany>(definition.arguments),
            "updateOne" => deserialize_op::<UpdateOne>(definition.arguments),
            "updateMany" => deserialize_op::<UpdateMany>(definition.arguments),
            "deleteMany" => deserialize_op::<DeleteMany>(definition.arguments),
            "deleteOne" => deserialize_op::<DeleteOne>(definition.arguments),
            "find" => deserialize_op::<Find>(definition.arguments),
            "aggregate" => deserialize_op::<Aggregate>(definition.arguments),
            "distinct" => deserialize_op::<Distinct>(definition.arguments),
            "countDocuments" => deserialize_op::<CountDocuments>(definition.arguments),
            "estimatedDocumentCount" => {
                deserialize_op::<EstimatedDocumentCount>(definition.arguments)
            }
            "findOne" => deserialize_op::<FindOne>(definition.arguments),
            "listCollections" => deserialize_op::<ListCollections>(definition.arguments),
            "listCollectionNames" => deserialize_op::<ListCollectionNames>(definition.arguments),
            "replaceOne" => deserialize_op::<ReplaceOne>(definition.arguments),
            "findOneAndUpdate" => deserialize_op::<FindOneAndUpdate>(definition.arguments),
            "findOneAndReplace" => deserialize_op::<FindOneAndReplace>(definition.arguments),
            "findOneAndDelete" => deserialize_op::<FindOneAndDelete>(definition.arguments),
            "listDatabases" => deserialize_op::<ListDatabases>(definition.arguments),
            "targetedFailPoint" => deserialize_op::<TargetedFailPoint>(definition.arguments),
            "assertSessionPinned" => deserialize_op::<AssertSessionPinned>(definition.arguments),
            "assertSessionUnpinned" => {
                deserialize_op::<AssertSessionUnpinned>(definition.arguments)
            }
            "listDatabaseNames" => deserialize_op::<ListDatabaseNames>(definition.arguments),
            "assertSessionTransactionState" => {
                deserialize_op::<AssertSessionTransactionState>(definition.arguments)
            }
            "startTransaction" => deserialize_op::<StartTransaction>(definition.arguments),
            "commitTransaction" => deserialize_op::<CommitTransaction>(definition.arguments),
            "abortTransaction" => deserialize_op::<AbortTransaction>(definition.arguments),
            "runCommand" => deserialize_op::<RunCommand>(definition.arguments),
            "dropCollection" => deserialize_op::<DropCollection>(definition.arguments),
            "createCollection" => deserialize_op::<CreateCollection>(definition.arguments),
            "assertCollectionExists" => {
                deserialize_op::<AssertCollectionExists>(definition.arguments)
            }
            "assertCollectionNotExists" => {
                deserialize_op::<AssertCollectionNotExists>(definition.arguments)
            }
            "createIndex" => deserialize_op::<CreateIndex>(definition.arguments),
            "dropIndex" => deserialize_op::<DropIndex>(definition.arguments),
            "listIndexes" => deserialize_op::<ListIndexes>(definition.arguments),
            "listIndexNames" => deserialize_op::<ListIndexNames>(definition.arguments),
            "assertIndexExists" => deserialize_op::<AssertIndexExists>(definition.arguments),
            "assertIndexNotExists" => deserialize_op::<AssertIndexNotExists>(definition.arguments),
            "watch" => deserialize_op::<Watch>(definition.arguments),
            "withTransaction" => deserialize_op::<WithTransaction>(definition.arguments),
            _ => Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>),
        }
        .map_err(|e| serde::de::Error::custom(format!("{}", e)))?;

        Ok(Operation {
            operation: boxed_op,
            name: definition.name,
            object: definition.object,
            collection_options: definition.collection_options,
            database_options: definition.database_options,
            error: definition.error,
            result: definition.result,
            session,
        })
    }
}

fn deserialize_op<'de, 'a, Op: TestOperation + Deserialize<'de> + 'a>(
    arguments: Document,
) -> std::result::Result<Box<dyn TestOperation + 'a>, bson::de::Error> {
    Ok(Box::new(Op::deserialize(BsonDeserializer::new(
        Bson::Document(arguments),
    ))?))
}

impl Deref for Operation {
    type Target = Box<dyn TestOperation>;

    fn deref(&self) -> &Box<dyn TestOperation> {
        &self.operation
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteMany {
    filter: Document,
    #[serde(flatten)]
    options: Option<DeleteOptions>,
}

impl TestOperation for DeleteMany {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .delete_many_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .delete_many(self.filter.clone(), self.options.clone())
                        .await?
                }
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteOne {
    filter: Document,
    #[serde(flatten)]
    options: Option<DeleteOptions>,
}

impl TestOperation for DeleteOne {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
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
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Find {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOptions>,
}

impl TestOperation for Find {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    let mut cursor = collection
                        .find_with_session(self.filter.clone(), self.options.clone(), session)
                        .await?;
                    cursor
                        .stream(session)
                        .try_collect::<Vec<Document>>()
                        .await?
                }
                None => {
                    let cursor = collection
                        .find(self.filter.clone(), self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<Document>>().await?
                }
            };
            Ok(Some(Bson::from(result)))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct InsertMany {
    documents: Vec<Document>,
    #[serde(flatten)]
    options: Option<InsertManyOptions>,
}

impl TestOperation for InsertMany {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        let documents = self.documents.clone();
        let options = self.options.clone();

        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .insert_many_with_session(documents, options, session)
                        .await?
                }
                None => collection.insert_many(documents, options).await?,
            };
            let ids: HashMap<String, Bson> = result
                .inserted_ids
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect();
            let ids = bson::to_bson(&ids)?;
            Ok(Some(Bson::from(doc! { "insertedIds": ids })))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct InsertOne {
    document: Document,
    #[serde(flatten)]
    options: Option<InsertOneOptions>,
}

impl TestOperation for InsertOne {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        let document = self.document.clone();
        let options = self.options.clone();
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .insert_one_with_session(document, options, session)
                        .await?
                }
                None => collection.insert_one(document, options).await?,
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateMany {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

impl TestOperation for UpdateMany {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .update_many_with_session(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .update_many(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                        )
                        .await?
                }
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

impl TestOperation for UpdateOne {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .update_one_with_session(
                            self.filter.clone(),
                            self.update.clone(),
                            self.options.clone(),
                            session,
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
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    #[serde(flatten)]
    options: Option<AggregateOptions>,
}

impl TestOperation for Aggregate {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    let mut cursor = collection
                        .aggregate_with_session(
                            self.pipeline.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?;
                    cursor
                        .stream(session)
                        .try_collect::<Vec<Document>>()
                        .await?
                }
                None => {
                    let cursor = collection
                        .aggregate(self.pipeline.clone(), self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<Document>>().await?
                }
            };
            Ok(Some(Bson::from(result)))
        }
        .boxed()
    }

    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    let mut cursor = database
                        .aggregate_with_session(
                            self.pipeline.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?;
                    cursor
                        .stream(session)
                        .try_collect::<Vec<Document>>()
                        .await?
                }
                None => {
                    let cursor = database
                        .aggregate(self.pipeline.clone(), self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<Document>>().await?
                }
            };

            Ok(Some(Bson::from(result)))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Distinct {
    field_name: String,
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<DistinctOptions>,
}

impl TestOperation for Distinct {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
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
            Ok(Some(Bson::Array(result)))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CountDocuments {
    filter: Document,
    #[serde(flatten)]
    options: Option<CountOptions>,
}

impl TestOperation for CountDocuments {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
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
            Ok(Some(Bson::Int64(result.try_into().unwrap())))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: Option<EstimatedDocumentCountOptions>,
}

impl TestOperation for EstimatedDocumentCount {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        _session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = collection
                .estimated_document_count(self.options.clone())
                .await?;
            Ok(Some(Bson::Int64(result.try_into().unwrap())))
        }
        .boxed()
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct FindOne {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOneOptions>,
}

impl TestOperation for FindOne {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .find_one_with_session(self.filter.clone(), self.options.clone(), session)
                        .await?
                }
                None => {
                    collection
                        .find_one(self.filter.clone(), self.options.clone())
                        .await?
                }
            };
            match result {
                Some(result) => Ok(Some(Bson::from(result))),
                None => Ok(None),
            }
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollections {
    #[serde(flatten)]
    options: Option<ListCollectionsOptions>,
}

impl TestOperation for ListCollections {
    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    let mut cursor = database
                        .list_collections()
                        .with_options(self.options.clone())
                        .session(&mut *session)
                        .await?;
                    cursor.stream(session).try_collect::<Vec<_>>().await?
                }
                None => {
                    let cursor = database
                        .list_collections()
                        .with_options(self.options.clone())
                        .await?;
                    cursor.try_collect::<Vec<_>>().await?
                }
            };
            Ok(Some(bson::to_bson(&result)?))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

impl TestOperation for ListCollectionNames {
    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let builder = database
                .list_collection_names()
                .update_with(self.filter.clone(), |b, f| b.filter(f));
            let result = match session {
                Some(session) => builder.session(session).await?,
                None => builder.await?,
            };
            let result: Vec<Bson> = result.into_iter().map(|s| s.into()).collect();
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<ReplaceOptions>,
}

impl TestOperation for ReplaceOne {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .replace_one_with_session(
                            self.filter.clone(),
                            self.replacement.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .replace_one(
                            self.filter.clone(),
                            self.replacement.clone(),
                            self.options.clone(),
                        )
                        .await?
                }
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndUpdate {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<FindOneAndUpdateOptions>,
}

impl TestOperation for FindOneAndUpdate {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
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
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndReplace {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<FindOneAndReplaceOptions>,
}

impl TestOperation for FindOneAndReplace {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .find_one_and_replace_with_session(
                            self.filter.clone(),
                            self.replacement.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .find_one_and_replace(
                            self.filter.clone(),
                            self.replacement.clone(),
                            self.options.clone(),
                        )
                        .await?
                }
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndDelete {
    filter: Document,
    #[serde(flatten)]
    options: Option<FindOneAndDeleteOptions>,
}

impl TestOperation for FindOneAndDelete {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    collection
                        .find_one_and_delete_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?
                }
                None => {
                    collection
                        .find_one_and_delete(self.filter.clone(), self.options.clone())
                        .await?
                }
            };
            let result = bson::to_bson(&result)?;
            Ok(Some(result))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct TargetedFailPoint {
    fail_point: FailPoint,
}

impl TestOperation for TargetedFailPoint {
    fn execute_on_client<'a>(&'a self, _client: &'a TestClient) -> BoxFuture<Result<Option<Bson>>> {
        async move { Ok(Some(to_bson(&self.fail_point)?)) }.boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertSessionPinned {}

impl TestOperation for AssertSessionPinned {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            assert!(session.transaction.pinned_mongos().is_some());
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertSessionUnpinned {}

impl TestOperation for AssertSessionUnpinned {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            assert!(session.transaction.pinned_mongos().is_none());
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabases {
    #[serde(flatten)]
    options: Option<crate::db::options::ListDatabasesOptions>,
}

impl TestOperation for ListDatabases {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = client
                .list_databases()
                .with_options(self.options.clone())
                .await?;
            Ok(Some(bson::to_bson(&result)?))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabaseNames {
    #[serde(flatten)]
    options: Option<crate::db::options::ListDatabasesOptions>,
}

impl TestOperation for ListDatabaseNames {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = client
                .list_database_names()
                .with_options(self.options.clone())
                .await?;
            let result: Vec<Bson> = result.into_iter().map(|s| s.into()).collect();
            Ok(Some(result.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertSessionTransactionState {
    state: String,
}

impl TestOperation for AssertSessionTransactionState {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            match self.state.as_str() {
                "none" => assert!(matches!(session.transaction.state, TransactionState::None)),
                "starting" => assert!(matches!(
                    session.transaction.state,
                    TransactionState::Starting
                )),
                "in_progress" => assert!(matches!(
                    session.transaction.state,
                    TransactionState::InProgress
                )),
                "committed" => assert!(matches!(
                    session.transaction.state,
                    TransactionState::Committed { .. }
                )),
                "aborted" => assert!(matches!(
                    session.transaction.state,
                    TransactionState::Aborted
                )),
                other => panic!("Unknown transaction state: {}", other),
            }
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct StartTransaction {
    options: Option<TransactionOptions>,
}

impl TestOperation for StartTransaction {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            session
                .start_transaction(self.options.clone())
                .await
                .map(|_| None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CommitTransaction {}

impl TestOperation for CommitTransaction {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move { session.commit_transaction().await.map(|_| None) }.boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AbortTransaction {}

impl TestOperation for AbortTransaction {
    fn execute_on_session<'a>(
        &'a self,
        session: &'a mut ClientSession,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move { session.abort_transaction().await.map(|_| None) }.boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunCommand {
    command: Document,
    read_preference: Option<ReadPreference>,
}

impl TestOperation for RunCommand {
    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let selection_criteria = self
                .read_preference
                .as_ref()
                .map(|read_preference| SelectionCriteria::ReadPreference(read_preference.clone()));
            let result = match session {
                Some(session) => {
                    database
                        .run_command_with_session(self.command.clone(), selection_criteria, session)
                        .await
                }
                None => database.run_command(self.command.clone(), None).await,
            };
            result.map(|doc| Some(Bson::Document(doc)))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DropCollection {
    collection: String,
    #[serde(flatten)]
    options: Option<DropCollectionOptions>,
}

impl TestOperation for DropCollection {
    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = match session {
                Some(session) => {
                    database
                        .collection::<Document>(&self.collection)
                        .drop_with_session(self.options.clone(), session)
                        .await
                }
                None => {
                    database
                        .collection::<Document>(&self.collection)
                        .drop(self.options.clone())
                        .await
                }
            };
            result.map(|_| None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CreateCollection {
    collection: String,
    #[serde(flatten)]
    options: Option<CreateCollectionOptions>,
}

impl TestOperation for CreateCollection {
    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = database
                .create_collection(&self.collection)
                .with_options(self.options.clone())
                .update_with(session, |b, s| b.session(s))
                .await;
            result.map(|_| None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertCollectionExists {
    database: String,
    collection: String,
}

impl TestOperation for AssertCollectionExists {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let collections = client
                .database_with_options(
                    &self.database,
                    DatabaseOptions::builder()
                        .read_concern(ReadConcern::majority())
                        .build(),
                )
                .list_collection_names()
                .await
                .unwrap();
            assert!(
                collections.contains(&self.collection),
                "Collection {}.{} should exist, but does not (collections: {:?}).",
                self.database,
                self.collection,
                collections
            );
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertCollectionNotExists {
    database: String,
    collection: String,
}

impl TestOperation for AssertCollectionNotExists {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let collections = client
                .database(&self.database)
                .list_collection_names()
                .await
                .unwrap();
            assert!(!collections.contains(&self.collection));
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateIndex {
    keys: Document,
    name: Option<String>,
}

impl TestOperation for CreateIndex {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let options = IndexOptions::builder().name(self.name.clone()).build();
            let index = IndexModel::builder()
                .keys(self.keys.clone())
                .options(options)
                .build();

            let name = match session {
                Some(session) => {
                    collection
                        .create_index_with_session(index, None, session)
                        .await?
                        .index_name
                }
                None => collection.create_index(index, None).await?.index_name,
            };
            Ok(Some(name.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DropIndex {
    name: String,
    #[serde(flatten)]
    options: DropIndexOptions,
}

impl TestOperation for DropIndex {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            match session {
                Some(session) => {
                    collection
                        .drop_index_with_session(self.name.clone(), self.options.clone(), session)
                        .await?
                }
                None => {
                    collection
                        .drop_index(self.name.clone(), self.options.clone())
                        .await?
                }
            }
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexes {
    #[serde(flatten)]
    options: ListIndexesOptions,
}

impl TestOperation for ListIndexes {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let indexes: Vec<IndexModel> = match session {
                Some(session) => {
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
            Ok(Some(indexes.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListIndexNames {}

impl TestOperation for ListIndexNames {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let names = match session {
                Some(session) => collection.list_index_names_with_session(session).await?,
                None => collection.list_index_names().await?,
            };
            Ok(Some(names.into()))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct Watch {}

impl TestOperation for Watch {
    fn execute_on_collection<'a>(
        &'a self,
        collection: &'a Collection<Document>,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            match session {
                None => {
                    collection.watch().await?;
                }
                Some(s) => {
                    collection.watch().session(s).await?;
                }
            }
            Ok(None)
        }
        .boxed()
    }

    fn execute_on_database<'a>(
        &'a self,
        database: &'a Database,
        session: Option<&'a mut ClientSession>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            match session {
                None => {
                    database.watch().await?;
                }
                Some(s) => {
                    database.watch().session(s).await?;
                }
            }
            Ok(None)
        }
        .boxed()
    }

    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            client.watch().await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertIndexExists {
    database: String,
    collection: String,
    index: String,
}

impl TestOperation for AssertIndexExists {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let coll = client
                .database(&self.database)
                .collection::<Document>(&self.collection);
            let indexes = coll.list_index_names().await?;
            assert!(indexes.contains(&self.index));
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct AssertIndexNotExists {
    database: String,
    collection: String,
    index: String,
}

impl TestOperation for AssertIndexNotExists {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let coll = client
                .database(&self.database)
                .collection::<Document>(&self.collection);
            match coll.list_index_names().await {
                Ok(indexes) => assert!(!indexes.contains(&self.index)),
                // a namespace not found error indicates that the index does not exist
                Err(err) => assert_eq!(err.sdam_code(), Some(26)),
            }
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct WithTransaction {
    callback: WithTransactionCallback,
    options: Option<TransactionOptions>,
}

#[derive(Debug, Deserialize)]
struct WithTransactionCallback {
    operations: Vec<Operation>,
}

impl TestOperation for WithTransaction {
    fn execute_recursive<'a>(
        &'a self,
        runner: &'a mut OpRunner,
        sessions: OpSessions<'a>,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let session = sessions.session0.unwrap();
            session
                .with_transaction(
                    (runner, &self.callback.operations, sessions.session1),
                    |session, (runner, operations, session1)| {
                        async move {
                            for op in operations.iter() {
                                let sessions = OpSessions {
                                    session0: Some(session),
                                    session1: session1.as_deref_mut(),
                                };
                                let result = match runner.run_operation(op, sessions).await {
                                    Some(r) => r,
                                    None => continue,
                                };
                                op.assert_result_matches(
                                    &result,
                                    "withTransaction nested operation",
                                );
                                // Propagate sub-operation errors after validating the result.
                                let _ = result?;
                            }
                            Ok(())
                        }
                        .boxed()
                    },
                    self.options.clone(),
                )
                .await?;
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

impl TestOperation for UnimplementedOperation {}
