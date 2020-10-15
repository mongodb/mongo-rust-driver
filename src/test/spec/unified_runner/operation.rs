use std::{collections::HashMap, fmt::Debug, ops::Deref};

use async_trait::async_trait;
use futures::stream::TryStreamExt;
use serde::{de::Deserializer, Deserialize};

use super::{ClientEntity, Entity, ExpectError, FailPointDisableCommand, TestRunner};

use crate::{
    bson::{doc, Bson, Deserializer as BsonDeserializer, Document},
    error::Result,
    options::{
        AggregateOptions,
        CountOptions,
        DeleteOptions,
        DistinctOptions,
        EstimatedDocumentCountOptions,
        FindOneAndDeleteOptions,
        FindOneAndReplaceOptions,
        FindOneAndUpdateOptions,
        FindOneOptions,
        FindOptions,
        InsertManyOptions,
        InsertOneOptions,
        ListCollectionsOptions,
        ListDatabasesOptions,
        ReadPreference,
        ReplaceOptions,
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
    },
    Collection,
    Database,
};

#[async_trait]
pub trait TestOperation: Debug {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>>;
}

#[derive(Debug)]
pub struct Operation {
    operation: Box<dyn TestOperation>,
    pub name: String,
    pub object: OperationObject,
    pub expect_error: Option<ExpectError>,
    pub expect_result: Option<Bson>,
    pub save_result_as_entity: Option<String>,
}

#[derive(Debug)]
pub enum OperationObject {
    TestRunner,
    Entity(String),
}

impl OperationObject {
    pub fn as_client<'a>(&self, entities: &'a HashMap<String, Entity>) -> &'a ClientEntity {
        match self {
            Self::Entity(id) => entities.get(id).unwrap().as_client(),
            Self::TestRunner => panic!("cannot convert TestRunner object to Client"),
        }
    }

    pub fn as_database<'a>(&self, entities: &'a HashMap<String, Entity>) -> &'a Database {
        match self {
            Self::Entity(id) => entities.get(id).unwrap().as_database(),
            Self::TestRunner => panic!("cannot convert TestRunner to Database"),
        }
    }

    pub fn as_collection<'a>(&self, entities: &'a HashMap<String, Entity>) -> &'a Collection {
        match self {
            Self::Entity(id) => entities.get(id).unwrap().as_collection(),
            Self::TestRunner => panic!("cannot convert TestRunner to Collection"),
        }
    }
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

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OperationDefinition {
            pub name: String,
            pub object: OperationObject,
            #[serde(default = "default_arguments")]
            pub arguments: Bson,
            pub expect_error: Option<ExpectError>,
            pub expect_result: Option<Bson>,
            pub save_result_as_entity: Option<String>,
        }

        fn default_arguments() -> Bson {
            Bson::Document(doc! {})
        }

        let definition = OperationDefinition::deserialize(deserializer)?;
        let boxed_op = match definition.name.as_str() {
            "insertOne" => InsertOne::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "insertMany" => InsertMany::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "updateOne" => UpdateOne::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "updateMany" => UpdateMany::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "deleteMany" => DeleteMany::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "deleteOne" => DeleteOne::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "find" => Find::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "aggregate" => Aggregate::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "distinct" => Distinct::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "countDocuments" => {
                CountDocuments::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "estimatedDocumentCount" => {
                EstimatedDocumentCount::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "findOne" => FindOne::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabases" => {
                ListDatabases::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "listDatabaseNames" => {
                ListDatabaseNames::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "listCollections" => {
                ListCollections::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "listCollectionNames" => {
                ListCollectionNames::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "replaceOne" => ReplaceOne::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndUpdate" => {
                FindOneAndUpdate::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "findOneAndReplace" => {
                FindOneAndReplace::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "findOneAndDelete" => {
                FindOneAndDelete::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "failPoint" => FailPoint::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "assertCollectionExists" => {
                AssertCollectionExists::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "assertCollectionNotExists" => {
                AssertCollectionNotExists::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            _ => Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>),
        }
        .map_err(|e| serde::de::Error::custom(format!("{}", e)))?;

        Ok(Operation {
            operation: boxed_op,
            name: definition.name,
            object: definition.object,
            expect_error: definition.expect_error,
            expect_result: definition.expect_result,
            save_result_as_entity: definition.save_result_as_entity,
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

#[async_trait]
impl TestOperation for DeleteMany {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .delete_many(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteOne {
    filter: Document,
    #[serde(flatten)]
    options: Option<DeleteOptions>,
}

#[async_trait]
impl TestOperation for DeleteOne {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .delete_one(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Find {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOptions>,
}

#[async_trait]
impl TestOperation for Find {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let cursor = collection
            .find(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct InsertMany {
    documents: Vec<Document>,
    #[serde(flatten)]
    options: Option<InsertManyOptions>,
}

#[async_trait]
impl TestOperation for InsertMany {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .insert_many(self.documents.clone(), self.options.clone())
            .await?;
        let ids: HashMap<String, Bson> = result
            .inserted_ids
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let ids = bson::to_bson(&ids)?;
        Ok(Some(Bson::from(doc! { "insertedIds": ids }).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct InsertOne {
    document: Document,
    #[serde(flatten)]
    options: Option<InsertOneOptions>,
}

#[async_trait]
impl TestOperation for InsertOne {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .insert_one(self.document.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateMany {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

#[async_trait]
impl TestOperation for UpdateMany {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .update_many(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

#[async_trait]
impl TestOperation for UpdateOne {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .update_one(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    #[serde(flatten)]
    options: Option<AggregateOptions>,
}

#[async_trait]
impl TestOperation for Aggregate {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        match object {
            OperationObject::Entity(id) => {
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
                    other => panic!("cannot execute aggregate on {:?}", &other),
                };
                let result = cursor.try_collect::<Vec<Document>>().await?;
                Ok(Some(Bson::from(result).into()))
            }
            OperationObject::TestRunner => panic!("cannot execute aggregate on the test runner"),
        }
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

#[async_trait]
impl TestOperation for Distinct {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .distinct(&self.field_name, self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::Array(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct CountDocuments {
    filter: Document,
    #[serde(flatten)]
    options: Option<CountOptions>,
}

#[async_trait]
impl TestOperation for CountDocuments {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .count_documents(self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::from(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: Option<EstimatedDocumentCountOptions>,
}

#[async_trait]
impl TestOperation for EstimatedDocumentCount {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .estimated_document_count(self.options.clone())
            .await?;
        Ok(Some(Bson::from(result).into()))
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct FindOne {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOneOptions>,
}

#[async_trait]
impl TestOperation for FindOne {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .find_one(self.filter.clone(), self.options.clone())
            .await?;
        match result {
            Some(result) => Ok(Some(Bson::from(result).into())),
            None => Ok(Some(Entity::None)),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabases {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

#[async_trait]
impl TestOperation for ListDatabases {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let client = object.as_client(&test_runner.entities);
        let result = client
            .list_databases(self.filter.clone(), self.options.clone())
            .await?;
        let result: Vec<Bson> = result.iter().map(Bson::from).collect();
        Ok(Some(Bson::Array(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabaseNames {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

#[async_trait]
impl TestOperation for ListDatabaseNames {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let client = object.as_client(&test_runner.entities);
        let result = client
            .list_database_names(self.filter.clone(), self.options.clone())
            .await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::Array(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollections {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListCollectionsOptions>,
}

#[async_trait]
impl TestOperation for ListCollections {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let database = object.as_database(&test_runner.entities);
        let cursor = database
            .list_collections(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

#[async_trait]
impl TestOperation for ListCollectionNames {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let database = object.as_database(&test_runner.entities);
        let result = database.list_collection_names(self.filter.clone()).await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::from(result).into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<ReplaceOptions>,
}

#[async_trait]
impl TestOperation for ReplaceOne {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .replace_one(
                self.filter.clone(),
                self.replacement.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndUpdate {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<FindOneAndUpdateOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndUpdate {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .find_one_and_update(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndReplace {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<FindOneAndReplaceOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndReplace {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .find_one_and_replace(
                self.filter.clone(),
                self.replacement.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct FindOneAndDelete {
    filter: Document,
    #[serde(flatten)]
    options: Option<FindOneAndDeleteOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndDelete {
    async fn execute<'a>(
        &self,
        object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = object.as_collection(&test_runner.entities);
        let result = collection
            .find_one_and_delete(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result.into()))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FailPoint {
    fail_point: Document,
    client: String,
}

#[async_trait]
impl TestOperation for FailPoint {
    async fn execute<'a>(
        &self,
        _object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        let client = test_runner.entities.get(&self.client).unwrap().as_client();
        client
            .database("admin")
            .run_command(self.fail_point.clone(), selection_criteria)
            .await
            .unwrap();

        let disable = FailPointDisableCommand {
            command: doc! {
                "configureFailPoint": self.fail_point.get_str("configureFailPoint").unwrap(),
                "mode": "off",
            },
            client: self.client.clone(),
        };
        test_runner.failpoint_disable_commands.push(disable);

        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AssertCollectionExists {
    collection_name: String,
    database_name: String,
}

#[async_trait]
impl TestOperation for AssertCollectionExists {
    async fn execute<'a>(
        &self,
        _object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let db = test_runner.internal_client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(names.contains(&self.collection_name));
        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AssertCollectionNotExists {
    collection_name: String,
    database_name: String,
}

#[async_trait]
impl TestOperation for AssertCollectionNotExists {
    async fn execute<'a>(
        &self,
        _object: &OperationObject,
        test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        let db = test_runner.internal_client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(!names.contains(&self.collection_name));
        Ok(None)
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

#[async_trait]
impl TestOperation for UnimplementedOperation {
    async fn execute<'a>(
        &self,
        _object: &OperationObject,
        _test_runner: &'a mut TestRunner,
    ) -> Result<Option<Entity>> {
        unimplemented!()
    }
}
