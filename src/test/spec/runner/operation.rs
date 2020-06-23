use std::{fmt::Debug, ops::Deref, time::Duration};

use async_trait::async_trait;
use futures::stream::TryStreamExt;
use serde::{
    de::{self, Deserializer},
    Deserialize,
};

use crate::{
    bson::{doc, Bson, Deserializer as BsonDeserializer, Document},
    bson_util,
    error::Result,
    options::{
        AggregateOptions,
        CollectionOptions,
        FindOptions,
        Hint,
        InsertManyOptions,
        ReplaceOptions,
        UpdateModifications,
        UpdateOptions,
    },
    test::{
        util::{CommandEvent, EventClient},
        OperationObject,
    },
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
pub struct AnyTestOperation {
    operation: Box<dyn TestOperation>,
    pub name: String,
    pub object: Option<OperationObject>,
    pub result: Option<Bson>,
    pub error: Option<bool>,
    pub collection_options: Option<CollectionOptions>,
}

impl<'de> Deserialize<'de> for AnyTestOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct OperationDefinition {
            name: String,
            arguments: Option<Bson>,
            object: Option<OperationObject>,
            result: Option<Bson>,
            error: Option<bool>,
            collection_options: Option<CollectionOptions>,
        };

        let definition = OperationDefinition::deserialize(deserializer)?;
        let boxed_op = match definition.name.as_str() {
            "insertOne" => {
                InsertOne::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "insertMany" => {
                InsertMany::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "updateOne" => {
                UpdateOne::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "updateMany" => {
                UpdateMany::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "deleteMany" => {
                DeleteMany::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "deleteOne" => {
                DeleteOne::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "find" => Find::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "aggregate" => {
                Aggregate::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "distinct" => {
                Distinct::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "countDocuments" => {
                CountDocuments::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "estimatedDocumentCount" => {
                Ok(Box::new(EstimatedDocumentCount) as Box<dyn TestOperation>)
            }
            "findOne" => FindOne::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            "listDatabases" => Ok(Box::new(ListDatabases) as Box<dyn TestOperation>),
            "listDatabaseNames" => Ok(Box::new(ListDatabaseNames) as Box<dyn TestOperation>),
            "listCollections" => Ok(Box::new(ListCollections) as Box<dyn TestOperation>),
            "listCollectionNames" => Ok(Box::new(ListCollectionNames) as Box<dyn TestOperation>),
            "replaceOne" => ReplaceOne::deserialize(BsonDeserializer::new(definition.arguments.unwrap()))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
            // isabeltodo
            // other => {dbg!("{}", other); Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>)},
            _ => Ok(Box::new(UnimplementedOperation) as Box<dyn TestOperation>),
        }
        .map_err(|e| de::Error::custom(format!("{}", e)))?;

        Ok(AnyTestOperation {
            operation: boxed_op,
            name: definition.name,
            object: definition.object,
            result: definition.result,
            error: definition.error,
            collection_options: definition.collection_options,
        })
    }
}

impl Deref for AnyTestOperation {
    type Target = Box<dyn TestOperation>;

    fn deref(&self) -> &Box<dyn TestOperation> {
        &self.operation
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct DeleteMany {
    filter: Document,
}

#[async_trait]
impl TestOperation for DeleteMany {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection.delete_many(self.filter.clone(), None).await?;
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
}

#[async_trait]
impl TestOperation for DeleteOne {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection.delete_one(self.filter.clone(), None).await?;
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

// This struct is necessary because the command monitoring tests specify the options in a very old
// way (SPEC-1519).
#[derive(Debug, Deserialize, Default)]
struct FindModifiers {
    #[serde(rename = "$comment", default)]
    comment: Option<String>,
    #[serde(rename = "$hint", default)]
    hint: Option<Hint>,
    #[serde(
        rename = "$maxTimeMS",
        deserialize_with = "bson_util::deserialize_duration_from_u64_millis",
        default
    )]
    max_time: Option<Duration>,
    #[serde(rename = "$min", default)]
    min: Option<Document>,
    #[serde(rename = "$max", default)]
    max: Option<Document>,
    #[serde(rename = "$returnKey", default)]
    return_key: Option<bool>,
    #[serde(rename = "$showDiskLoc", default)]
    show_disk_loc: Option<bool>,
}

impl FindModifiers {
    fn update_options(&self, options: &mut FindOptions) {
        options.comment = self.comment.clone();
        options.hint = self.hint.clone();
        options.max_time = self.max_time;
        options.min = self.min.clone();
        options.max = self.max.clone();
        options.return_key = self.return_key;
        options.show_record_id = self.show_disk_loc;
    }
}

#[derive(Debug, Default, Deserialize)]
pub(super) struct Find {
    filter: Option<Document>,
    #[serde(default)]
    sort: Option<Document>,
    #[serde(default)]
    skip: Option<i64>,
    #[serde(default, rename = "batchSize")]
    batch_size: Option<i64>,
    #[serde(default)]
    limit: Option<i64>,
    #[serde(default)]
    modifiers: Option<FindModifiers>,
}

#[async_trait]
impl TestOperation for Find {
    fn command_names(&self) -> &[&str] {
        &["find", "getMore"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let mut options = FindOptions {
            sort: self.sort.clone(),
            skip: self.skip,
            batch_size: self.batch_size.map(|i| i as u32),
            limit: self.limit,
            ..Default::default()
        };

        if let Some(ref modifiers) = self.modifiers {
            modifiers.update_options(&mut options);
        }

        let cursor = collection.find(self.filter.clone(), options).await?;
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
    #[serde(default)]
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
pub(super) struct InsertOne {
    document: Document,
}

#[async_trait]
impl TestOperation for InsertOne {
    fn command_names(&self) -> &[&str] {
        &["insert"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection.insert_one(self.document.clone(), None).await?;
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
    #[serde(default)]
    hint: Option<Hint>,
}

#[async_trait]
impl TestOperation for UpdateMany {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let options = UpdateOptions {
            hint: self.hint.clone(),
            ..Default::default()
        };
        let result = collection
            .update_many(self.filter.clone(), self.update.clone(), options)
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
    #[serde(default)]
    upsert: Option<bool>,
    #[serde(default)]
    hint: Option<Hint>,
}

#[async_trait]
impl TestOperation for UpdateOne {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let options = UpdateOptions {
            upsert: self.upsert,
            hint: self.hint.clone(),
            ..Default::default()
        };
        let result = collection
            .update_one(self.filter.clone(), self.update.clone(), options)
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
}

#[async_trait]
impl TestOperation for Aggregate {
    fn command_names(&self) -> &[&str] {
        &["aggregate"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let cursor = collection.aggregate(self.pipeline.clone(), None).await?;
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
#[serde(rename_all = "camelCase")]
pub(super) struct Distinct {
    field_name: String,
    #[serde(default)]
    filter: Option<Document>,
}

#[async_trait]
impl TestOperation for Distinct {
    fn command_names(&self) -> &[&str] {
        &["distinct"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .distinct(&self.field_name, self.filter.clone(), None)
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
}

#[async_trait]
impl TestOperation for CountDocuments {
    fn command_names(&self) -> &[&str] {
        &["aggregate"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection
            .count_documents(self.filter.clone(), None)
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
pub(super) struct EstimatedDocumentCount;

#[async_trait]
impl TestOperation for EstimatedDocumentCount {
    fn command_names(&self) -> &[&str] {
        &["count"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection.estimated_document_count(None).await?;
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
}

#[async_trait]
impl TestOperation for FindOne {
    fn command_names(&self) -> &[&str] {
        &["find"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let result = collection.find_one(self.filter.clone(), None).await?;
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
pub(super) struct ListDatabases;

#[async_trait]
impl TestOperation for ListDatabases {
    fn command_names(&self) -> &[&str] {
        &["listDatabases"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>> {
        let result = client.list_databases(None, None).await?;
        let result: Vec<Bson> = result.iter().map(Bson::from).collect();
        Ok(Some(Bson::Array(result)))
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListDatabaseNames;

#[async_trait]
impl TestOperation for ListDatabaseNames {
    fn command_names(&self) -> &[&str] {
        &["listDatabases"]
    }

    async fn execute_on_collection(&self, _collection: &Collection) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>> {
        let result = client.list_database_names(None, None).await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::Array(result)))
    }

    async fn execute_on_database(&self, _database: &Database) -> Result<Option<Bson>> {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollections;

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
        let cursor = database.list_collections(None, None).await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result)))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ListCollectionNames;

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
        let result = database.list_collection_names(None).await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::from(result)))
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    #[serde(default)]
    hint: Option<Hint>,
}

#[async_trait]
impl TestOperation for ReplaceOne {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute_on_collection(&self, collection: &Collection) -> Result<Option<Bson>> {
        let options = ReplaceOptions {
            hint: self.hint.clone(),
            ..Default::default()
        };
        let result = collection
            .replace_one(self.filter.clone(), self.replacement.clone(), options)
            .await?;
        let result = bson::to_bson(&result)?;
        Ok(Some(result))
    }

    async fn execute_on_client(&self, client: &EventClient) -> Result<Option<Bson>> {
        unimplemented!()
    }

    async fn execute_on_database(&self, database: &Database) -> Result<Option<Bson>> {
        let options = AggregateOptions {
            allow_disk_use: Some(true),
            ..Default::default()
        };
        let cursor = database.aggregate(self.pipeline.clone(), None).await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        // dbg!("{:?}", &result);
        Ok(Some(Bson::from(result)))
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

impl EventClient {
    pub async fn run_database_operation(
        &self,
        operation: &AnyTestOperation,
        database_name: &str,
    ) -> Result<Option<Bson>> {
        operation
            .execute_on_database(&self.database(database_name))
            .await
    }

    pub async fn run_collection_operation(
        &self,
        operation: &AnyTestOperation,
        db_name: &str,
        coll_name: &str,
        collection_options: Option<CollectionOptions>,
    ) -> Result<Option<Bson>> {
        let coll = match collection_options {
            Some(options) => self
                .database(db_name)
                .collection_with_options(coll_name, options),
            None => self.database(db_name).collection(coll_name),
        };
        operation.execute_on_collection(&coll).await
    }

    pub async fn run_client_operation(&self, operation: &AnyTestOperation) -> Result<Option<Bson>> {
        operation.execute_on_client(self).await
    }

    pub fn collect_events(&self, operation: &AnyTestOperation) -> Vec<CommandEvent> {
        self
            .command_events
            .write()
            .unwrap()
            .drain(..)
            .filter(|event| {
                event.is_command_started()
                    && operation.command_names().contains(&event.command_name())
            })
            .collect()
    }
}
