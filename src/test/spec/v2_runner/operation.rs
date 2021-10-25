use std::{collections::HashMap, convert::TryInto, fmt::Debug, ops::Deref};

use futures::{future::BoxFuture, stream::TryStreamExt, FutureExt};
use serde::{de::Deserializer, Deserialize};

use crate::{
    bson::{doc, to_bson, Bson, Deserializer as BsonDeserializer, Document},
    client::session::TransactionState,
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
        ListCollectionsOptions,
        ListDatabasesOptions,
        ListIndexesOptions,
        ReplaceOptions,
        TransactionOptions,
        UpdateModifications,
        UpdateOptions,
        WriteConcern,
    },
    selection_criteria::{ReadPreference, SelectionCriteria},
    test::{FailPoint, TestClient},
    ClientSession,
    Collection,
    Database,
    IndexModel,
};

pub trait TestOperation: Debug {
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
}

#[derive(Debug)]
pub struct Operation {
    operation: Box<dyn TestOperation>,
    pub name: String,
    pub object: Option<OperationObject>,
    pub collection_options: Option<CollectionOptions>,
    pub database_options: Option<DatabaseOptions>,
    pub error: Option<bool>,
    pub result: Option<OperationResult>,
    pub session: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub enum OperationObject {
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
pub enum OperationResult {
    Error(OperationError),
    Success(Bson),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationError {
    pub error_contains: Option<String>,
    pub error_code_name: Option<String>,
    pub error_code: Option<i32>,
    pub error_labels_contain: Option<Vec<String>>,
    pub error_labels_omit: Option<Vec<String>>,
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        struct OperationDefinition {
            pub name: String,
            pub object: Option<OperationObject>,
            pub collection_options: Option<CollectionOptions>,
            pub database_options: Option<DatabaseOptions>,
            #[serde(default = "default_arguments")]
            pub arguments: Document,
            pub error: Option<bool>,
            pub result: Option<OperationResult>,
            // We don't need to use this field, but it needs to be included during deserialization
            // so that we can use the deny_unknown_fields tag.
            #[serde(rename = "command_name")]
            pub _command_name: Option<String>,
        }

        fn default_arguments() -> Document {
            doc! {}
        }

        let mut definition = OperationDefinition::deserialize(deserializer)?;
        let session = definition
            .arguments
            .remove("session")
            .map(|session| session.as_str().unwrap().to_string());

        // TODO RUST-829 remove this once we handle default write concerns properly
        if let Some(ref mut collection_options) = definition.collection_options {
            if collection_options.write_concern == Some(WriteConcern::builder().build()) {
                collection_options.write_concern = None;
            }
        }

        // TODO RUST-829 remove this once we handle default write concerns properly
        if let Some(ref mut database_options) = definition.database_options {
            if database_options.write_concern == Some(WriteConcern::builder().build()) {
                database_options.write_concern = None;
            }
        }

        let boxed_op = match definition.name.as_str() {
            "insertOne" => {
                InsertOne::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "insertMany" => {
                InsertMany::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "updateOne" => {
                UpdateOne::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "updateMany" => {
                UpdateMany::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "deleteMany" => {
                DeleteMany::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "deleteOne" => {
                DeleteOne::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "find" => {
                Find::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "aggregate" => {
                Aggregate::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "distinct" => {
                Distinct::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "countDocuments" => CountDocuments::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "estimatedDocumentCount" => EstimatedDocumentCount::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOne" => {
                FindOne::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "listCollections" => ListCollections::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listCollectionNames" => ListCollectionNames::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "replaceOne" => {
                ReplaceOne::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "findOneAndUpdate" => FindOneAndUpdate::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndReplace" => FindOneAndReplace::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndDelete" => FindOneAndDelete::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabases" => ListDatabases::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "targetedFailPoint" => TargetedFailPoint::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertSessionPinned" => AssertSessionPinned::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertSessionUnpinned" => AssertSessionUnpinned::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabaseNames" => ListDatabaseNames::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertSessionTransactionState" => AssertSessionTransactionState::deserialize(
                BsonDeserializer::new(Bson::Document(definition.arguments)),
            )
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "startTransaction" => StartTransaction::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "commitTransaction" => CommitTransaction::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "abortTransaction" => AbortTransaction::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "runCommand" => {
                RunCommand::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "dropCollection" => DropCollection::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "createCollection" => CreateCollection::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertCollectionExists" => AssertCollectionExists::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertCollectionNotExists" => AssertCollectionNotExists::deserialize(
                BsonDeserializer::new(Bson::Document(definition.arguments)),
            )
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "createIndex" => CreateIndex::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "dropIndex" => {
                DropIndex::deserialize(BsonDeserializer::new(Bson::Document(definition.arguments)))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "listIndexes" => ListIndexes::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listIndexNames" => ListIndexNames::deserialize(BsonDeserializer::new(Bson::Document(
                definition.arguments,
            )))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertIndexExists" => AssertIndexExists::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertIndexNotExists" => AssertIndexNotExists::deserialize(BsonDeserializer::new(
                Bson::Document(definition.arguments),
            ))
            .map(|op| Box::new(op) as Box<dyn TestOperation>),
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
    filter: Option<Document>,
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
                        .list_collections_with_session(
                            self.filter.clone(),
                            self.options.clone(),
                            session,
                        )
                        .await?;
                    cursor.stream(session).try_collect::<Vec<_>>().await?
                }
                None => {
                    let cursor = database
                        .list_collections(self.filter.clone(), self.options.clone())
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
            let result = match session {
                Some(session) => {
                    database
                        .list_collection_names_with_session(self.filter.clone(), session)
                        .await?
                }
                None => database.list_collection_names(self.filter.clone()).await?,
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
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

impl TestOperation for ListDatabases {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = client
                .list_databases(self.filter.clone(), self.options.clone())
                .await?;
            Ok(Some(bson::to_bson(&result)?))
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabaseNames {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

impl TestOperation for ListDatabaseNames {
    fn execute_on_client<'a>(
        &'a self,
        client: &'a TestClient,
    ) -> BoxFuture<'a, Result<Option<Bson>>> {
        async move {
            let result = client
                .list_database_names(self.filter.clone(), self.options.clone())
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
            let result = match session {
                Some(session) => {
                    database
                        .create_collection_with_session(
                            &self.collection,
                            self.options.clone(),
                            session,
                        )
                        .await
                }
                None => {
                    database
                        .create_collection(&self.collection, self.options.clone())
                        .await
                }
            };
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
                .database(&self.database)
                .list_collection_names(None)
                .await
                .unwrap();
            assert!(collections.contains(&self.collection));
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
                .list_collection_names(None)
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
                Err(err) => assert_eq!(err.code(), Some(26)),
            }
            Ok(None)
        }
        .boxed()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

impl TestOperation for UnimplementedOperation {}
