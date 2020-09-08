use std::{collections::HashMap, fmt::Debug, ops::Deref};

use async_trait::async_trait;
use futures::stream::TryStreamExt;
use serde::Deserialize;

use super::{ExpectedError, Operation};

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
        ReplaceOptions,
        UpdateModifications,
        UpdateOptions,
    },
    test::util::EventClient,
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
}

#[derive(Debug)]
pub struct EntityOperation {
    operation: Box<dyn TestOperation>,
    pub name: String,
    pub object: String,
    pub expected_error: Option<ExpectedError>,
    pub expected_result: Option<Bson>,
    pub save_result_as_entity: Option<String>,
}

impl EntityOperation {
    pub fn from_operation(operation: Operation) -> Result<Self> {
        let arguments = match operation.arguments {
            Some(arguments) => Bson::from(arguments),
            None => Bson::from(doc! {}),
        };
        let boxed_op = match operation.name.as_str() {
            "insertOne" => InsertOne::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "insertMany" => InsertMany::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "updateOne" => UpdateOne::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "updateMany" => UpdateMany::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "deleteMany" => DeleteMany::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "deleteOne" => DeleteOne::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "find" => Find::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "aggregate" => Aggregate::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "distinct" => Distinct::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "countDocuments" => CountDocuments::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "estimatedDocumentCount" => {
                EstimatedDocumentCount::deserialize(BsonDeserializer::new(arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "findOne" => FindOne::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabases" => ListDatabases::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabaseNames" => ListDatabaseNames::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listCollections" => ListCollections::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listCollectionNames" => {
                ListCollectionNames::deserialize(BsonDeserializer::new(arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "replaceOne" => ReplaceOne::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndUpdate" => FindOneAndUpdate::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndReplace" => FindOneAndReplace::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "findOneAndDelete" => FindOneAndDelete::deserialize(BsonDeserializer::new(arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            _ => Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>),
        }?;

        Ok(EntityOperation {
            operation: boxed_op,
            name: operation.name,
            object: operation.object,
            expected_error: operation.expected_error,
            expected_result: operation.expected_result,
            save_result_as_entity: operation.save_result_as_entity,
        })
    }
}

impl Deref for EntityOperation {
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
}
