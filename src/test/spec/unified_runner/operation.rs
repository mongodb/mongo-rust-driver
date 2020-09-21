use std::{collections::HashMap, fmt::Debug, ops::Deref};

use async_trait::async_trait;
use futures::stream::TryStreamExt;
use serde::{de::Deserializer, Deserialize};

use super::{Entity, ExpectError};

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
    test::{util::EventClient, TestClient},
    Collection,
    Database,
};

#[async_trait]
pub trait TestOperation: Debug {
    /// The command names to monitor as part of this test.
    fn command_names(&self) -> &[&str];

    // The linked issue causes a warning that cannot be suppressed when providing a default
    // implementation for these functions.
    // <https://github.com/rust-lang/rust/issues/51443>
    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>>;

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>>;

    async fn execute_on_database(&self, database: &Database) -> Result<Option<Bson>>;

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    );
}

#[derive(Debug)]
pub struct Operation {
    operation: Box<dyn TestOperation>,
    pub arguments: Document,
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
        let original_arguments = definition.arguments.as_document().cloned().unwrap();
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
            arguments: original_arguments,
            name: definition.name,
            object: definition.object,
            expect_error: definition.expect_error,
            expect_result: definition.expect_result,
            save_result_as_entity: definition.save_result_as_entity,
        })
    }
}

impl Entity {
    pub async fn execute_operation(&self, operation: &Operation) -> Result<Option<Bson>> {
        match self {
            Entity::Client(client) => operation.execute_on_client(client).await,
            Entity::Database(db) => operation.execute_on_database(db).await,
            Entity::Collection(coll) => operation.execute_on_collection(coll).await,
            _ => panic!("Cannot execute operation on entity"),
        }
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
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .delete_many(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .delete_one(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["find", "getMore"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let cursor = collection
            .find(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["insert"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .insert_many(self.documents.clone(), self.options.clone())
            .await?;
        let ids: HashMap<String, Bson> = result
            .inserted_ids
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let ids = bson::to_bson(&ids)?;
        Ok(Some(Bson::from(doc! { "insertedIds": ids })))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["insert"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .insert_one(self.document.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .update_many(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .update_one(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["aggregate"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let cursor = collection
            .aggregate(self.pipeline.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, database: &Database) -> Result<Option<Bson>> {
        let cursor = database
            .aggregate(self.pipeline.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["distinct"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .distinct(&self.field_name, self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::Array(result)))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["aggregate"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .count_documents(self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: Option<EstimatedDocumentCountOptions>,
}

#[async_trait]
impl TestOperation for EstimatedDocumentCount {
    fn command_names(&self) -> &[&str] {
        &["count"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .estimated_document_count(self.options.clone())
            .await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["find"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .find_one(self.filter.clone(), self.options.clone())
            .await?;
        match result {
            Some(result) => Ok(Some(Bson::from(result))),
            None => Ok(None),
        }
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["listDatabases"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>> {
        let result = client
            .list_databases(self.filter.clone(), self.options.clone())
            .await?;
        let result: Vec<Bson> = result.iter().map(Bson::from).collect();
        Ok(Some(Bson::Array(result)))
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["listDatabases"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>> {
        let result = client
            .list_database_names(self.filter.clone(), self.options.clone())
            .await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::Array(result)))
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["listCollections"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, database: &Database) -> Result<Option<Bson>> {
        let cursor = database
            .list_collections(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

#[async_trait]
impl TestOperation for ListCollectionNames {
    fn command_names(&self) -> &[&str] {
        &["listCollections"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, database: &Database) -> Result<Option<Bson>> {
        let result = database.list_collection_names(self.filter.clone()).await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::from(result)))
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .replace_one(
                self.filter.clone(),
                self.replacement.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["findAndModify"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .find_one_and_update(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["findAndModify"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .find_one_and_replace(
                self.filter.clone(),
                self.replacement.clone(),
                self.options.clone(),
            )
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
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
    fn command_names(&self) -> &[&str] {
        &["findAndModify"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .find_one_and_delete(self.filter.clone(), self.options.clone())
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FailPoint {
    fail_point: Document,
    client_id: String,
}

#[async_trait]
impl TestOperation for FailPoint {
    fn command_names(&self) -> &[&str] {
        unimplemented!()
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        entities: &HashMap<String, Entity>,
    ) {
        let client = entities.get(&self.client_id).unwrap().as_client();
        let selection_criteria = SelectionCriteria::ReadPreference(ReadPreference::Primary);
        client
            .database("admin")
            .run_command(self.fail_point.clone(), selection_criteria)
            .await
            .unwrap();
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
    fn command_names(&self) -> &[&str] {
        unimplemented!()
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        let db = client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(names.contains(&self.collection_name));
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
    fn command_names(&self) -> &[&str] {
        unimplemented!()
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        let db = client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(!names.contains(&self.collection_name));
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

#[async_trait]
impl TestOperation for UnimplementedOperation {
    fn command_names(&self) -> &[&str] {
        unimplemented!()
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, _client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_test_runner(
        &self,
        _client: &TestClient,
        _entities: &HashMap<String, Entity>,
    ) {
        unimplemented!()
    }
}
