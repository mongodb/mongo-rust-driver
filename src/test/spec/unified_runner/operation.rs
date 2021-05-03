use std::{collections::HashMap, fmt::Debug, ops::Deref};

use async_trait::async_trait;
use futures::stream::TryStreamExt;
use serde::{de::Deserializer, Deserialize};

use super::{Entity, ExpectError, TestRunner};

use crate::{
    bson::{doc, to_bson, Bson, Deserializer as BsonDeserializer, Document},
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
        SelectionCriteria,
        UpdateModifications,
        UpdateOptions,
    },
    selection_criteria::ReadPreference,
    test::FailPoint,
};

#[async_trait]
pub trait TestOperation: Debug {
    async fn execute_test_runner_operation(&self, test_runner: &mut TestRunner);

    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>>;

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
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
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
            "failPoint" => {
                FailPointCommand::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "assertCollectionExists" => {
                AssertCollectionExists::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "assertCollectionNotExists" => {
                AssertCollectionNotExists::deserialize(BsonDeserializer::new(definition.arguments))
                    .map(|op| Box::new(op) as Box<dyn TestOperation>)
            }
            "runCommand" => RunCommand::deserialize(BsonDeserializer::new(definition.arguments))
                .map(|op| Box::new(op) as Box<dyn TestOperation>),
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
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteMany {
    filter: Document,
    #[serde(flatten)]
    options: Option<DeleteOptions>,
}

#[async_trait]
impl TestOperation for DeleteMany {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .delete_many(self.filter.clone(), self.options.clone())
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DeleteOne {
    filter: Document,
    #[serde(flatten)]
    options: Option<DeleteOptions>,
}

#[async_trait]
impl TestOperation for DeleteOne {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .delete_one(self.filter.clone(), self.options.clone())
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Find {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOptions>,
}

#[async_trait]
impl TestOperation for Find {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let cursor = collection
            .find(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result).into()))
    }

    fn returns_root_documents(&self) -> bool {
        false
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertMany {
    documents: Vec<Document>,
    #[serde(flatten)]
    options: Option<InsertManyOptions>,
}

#[async_trait]
impl TestOperation for InsertMany {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .insert_many(self.documents.clone(), self.options.clone())
            .await?;
        let ids: HashMap<String, Bson> = result
            .inserted_ids
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let ids = to_bson(&ids)?;
        Ok(Some(Bson::from(doc! { "insertedIds": ids }).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct InsertOne {
    document: Document,
    session: Option<String>,
    #[serde(flatten)]
    options: Option<InsertOneOptions>,
}

#[async_trait]
impl TestOperation for InsertOne {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .insert_one(self.document.clone(), self.options.clone())
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateMany {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

#[async_trait]
impl TestOperation for UpdateMany {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
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

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct UpdateOne {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<UpdateOptions>,
}

#[async_trait]
impl TestOperation for UpdateOne {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .update_one(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Aggregate {
    pipeline: Vec<Document>,
    #[serde(flatten)]
    options: Option<AggregateOptions>,
}

#[async_trait]
impl TestOperation for Aggregate {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
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
        let result = cursor.try_collect::<Vec<Document>>().await?;
        Ok(Some(Bson::from(result).into()))
    }

    fn returns_root_documents(&self) -> bool {
        true
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Distinct {
    field_name: String,
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<DistinctOptions>,
}

#[async_trait]
impl TestOperation for Distinct {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .distinct(&self.field_name, self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::Array(result).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct CountDocuments {
    filter: Document,
    #[serde(flatten)]
    options: Option<CountOptions>,
}

#[async_trait]
impl TestOperation for CountDocuments {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .count_documents(self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(Bson::from(result).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct EstimatedDocumentCount {
    #[serde(flatten)]
    options: Option<EstimatedDocumentCountOptions>,
}

#[async_trait]
impl TestOperation for EstimatedDocumentCount {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .estimated_document_count(self.options.clone())
            .await?;
        Ok(Some(Bson::from(result).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOne {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<FindOneOptions>,
}

#[async_trait]
impl TestOperation for FindOne {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .find_one(self.filter.clone(), self.options.clone())
            .await?;
        match result {
            Some(result) => Ok(Some(Bson::from(result).into())),
            None => Ok(Some(Entity::None)),
        }
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabases {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

#[async_trait]
impl TestOperation for ListDatabases {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let client = test_runner.get_client(id);
        let result = client
            .list_databases(self.filter.clone(), self.options.clone())
            .await?;
        Ok(Some(bson::to_bson(&result)?.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListDatabaseNames {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListDatabasesOptions>,
}

#[async_trait]
impl TestOperation for ListDatabaseNames {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let client = test_runner.get_client(id);
        let result = client
            .list_database_names(self.filter.clone(), self.options.clone())
            .await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::Array(result).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollections {
    filter: Option<Document>,
    #[serde(flatten)]
    options: Option<ListCollectionsOptions>,
}

#[async_trait]
impl TestOperation for ListCollections {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let db = test_runner.get_database(id);
        let cursor = db
            .list_collections(self.filter.clone(), self.options.clone())
            .await?;
        let result = cursor.try_collect::<Vec<_>>().await?;
        Ok(Some(bson::to_bson(&result)?.into()))
    }

    fn returns_root_documents(&self) -> bool {
        true
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ListCollectionNames {
    filter: Option<Document>,
}

#[async_trait]
impl TestOperation for ListCollectionNames {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let db = test_runner.get_database(id);
        let result = db.list_collection_names(self.filter.clone()).await?;
        let result: Vec<Bson> = result.iter().map(|s| Bson::String(s.to_string())).collect();
        Ok(Some(Bson::from(result).into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct ReplaceOne {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<ReplaceOptions>,
}

#[async_trait]
impl TestOperation for ReplaceOne {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
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

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndUpdate {
    filter: Document,
    update: UpdateModifications,
    #[serde(flatten)]
    options: Option<FindOneAndUpdateOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndUpdate {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .find_one_and_update(
                self.filter.clone(),
                self.update.clone(),
                self.options.clone(),
            )
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndReplace {
    filter: Document,
    replacement: Document,
    #[serde(flatten)]
    options: Option<FindOneAndReplaceOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndReplace {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
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

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FindOneAndDelete {
    filter: Document,
    #[serde(flatten)]
    options: Option<FindOneAndDeleteOptions>,
}

#[async_trait]
impl TestOperation for FindOneAndDelete {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let collection = test_runner.get_collection(id);
        let result = collection
            .find_one_and_delete(self.filter.clone(), self.options.clone())
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct FailPointCommand {
    fail_point: FailPoint,
    client: String,
}

#[async_trait]
impl TestOperation for FailPointCommand {
    async fn execute_test_runner_operation(&self, test_runner: &mut TestRunner) {
        let client = test_runner.get_client(&self.client);
        let guard = self
            .fail_point
            .clone()
            .enable(client, Some(ReadPreference::Primary.into()))
            .await
            .unwrap();
        test_runner.fail_point_guards.push(guard);
    }

    async fn execute_entity_operation(
        &self,
        _id: &str,
        _test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionExists {
    collection_name: String,
    database_name: String,
}

#[async_trait]
impl TestOperation for AssertCollectionExists {
    async fn execute_test_runner_operation(&self, test_runner: &mut TestRunner) {
        let db = test_runner.internal_client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(names.contains(&self.collection_name));
    }

    async fn execute_entity_operation(
        &self,
        _id: &str,
        _test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct AssertCollectionNotExists {
    collection_name: String,
    database_name: String,
}

#[async_trait]
impl TestOperation for AssertCollectionNotExists {
    async fn execute_test_runner_operation(&self, test_runner: &mut TestRunner) {
        let db = test_runner.internal_client.database(&self.database_name);
        let names = db.list_collection_names(None).await.unwrap();
        assert!(!names.contains(&self.collection_name));
    }

    async fn execute_entity_operation(
        &self,
        _id: &str,
        _test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct RunCommand {
    command: Document,
    command_name: String,
    read_concern: Option<Document>,
    read_preference: Option<SelectionCriteria>,
    session: Option<String>,
    write_concern: Option<Document>,
}

#[async_trait]
impl TestOperation for RunCommand {
    async fn execute_entity_operation(
        &self,
        id: &str,
        test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        let mut command = self.command.clone();
        if let Some(ref read_concern) = self.read_concern {
            command.insert("readConcern", read_concern.clone());
        }
        if let Some(ref write_concern) = self.write_concern {
            command.insert("writeConcern", write_concern.clone());
        }

        let db = test_runner.get_database(id);
        let result = db
            .run_command(command, self.read_preference.clone())
            .await?;
        let result = to_bson(&result)?;
        Ok(Some(result.into()))
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UnimplementedOperation;

#[async_trait]
impl TestOperation for UnimplementedOperation {
    async fn execute_entity_operation(
        &self,
        _id: &str,
        _test_runner: &mut TestRunner,
    ) -> Result<Option<Entity>> {
        unimplemented!()
    }

    async fn execute_test_runner_operation(&self, _test_runner: &mut TestRunner) {
        unimplemented!()
    }
}
