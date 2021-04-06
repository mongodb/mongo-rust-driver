use std::{ops::Deref, time::Duration};

use async_trait::async_trait;
use futures::stream::StreamExt;
use serde::{
    de::{self, Deserializer},
    Deserialize,
};

use crate::{
    bson::{Bson, Deserializer as BsonDeserializer, Document},
    bson_util,
    error::Result,
    options::{FindOptions, Hint, InsertManyOptions, UpdateOptions},
    test::util::{CommandEvent, EventClient},
    Collection,
};

#[async_trait]
pub(super) trait TestOperation {
    /// The command names to monitor as part of this test.
    fn command_names(&self) -> &[&str];

    async fn execute(&self, collection: Collection) -> Result<()>;
}

pub(super) struct AnyTestOperation {
    operation: Box<dyn TestOperation>,
}

impl<'de> Deserialize<'de> for AnyTestOperation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct OperationDefinition {
            name: String,
            arguments: Bson,
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
            _ => unimplemented!(),
        }
        .map_err(|e| de::Error::custom(format!("{}", e)))?;

        Ok(AnyTestOperation {
            operation: boxed_op,
        })
    }
}

impl Deref for AnyTestOperation {
    type Target = Box<dyn TestOperation>;

    fn deref(&self) -> &Box<dyn TestOperation> {
        &self.operation
    }
}

#[derive(Deserialize)]
pub(super) struct DeleteMany {
    filter: Document,
}

#[async_trait]
impl TestOperation for DeleteMany {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .delete_many(self.filter.clone(), None)
            .await
            .map(|_| ())
    }
}

#[derive(Deserialize)]
pub(super) struct DeleteOne {
    filter: Document,
}

#[async_trait]
impl TestOperation for DeleteOne {
    fn command_names(&self) -> &[&str] {
        &["delete"]
    }

    async fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .delete_one(self.filter.clone(), None)
            .await
            .map(|_| ())
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

    async fn execute(&self, collection: Collection) -> Result<()> {
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

        let mut cursor = collection.find(self.filter.clone(), options).await?;

        while let Some(result) = cursor.next().await {
            result?;
        }
        Ok(())
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

    async fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .insert_many(self.documents.clone(), self.options.clone())
            .await
            .map(|_| ())
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

    async fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .insert_one(self.document.clone(), None)
            .await
            .map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateMany {
    filter: Document,
    update: Document,
}

#[async_trait]
impl TestOperation for UpdateMany {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute(&self, collection: Collection) -> Result<()> {
        collection
            .update_many(self.filter.clone(), self.update.clone(), None)
            .await
            .map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
pub(super) struct UpdateOne {
    filter: Document,
    update: Document,
    #[serde(default)]
    upsert: Option<bool>,
}

#[async_trait]
impl TestOperation for UpdateOne {
    fn command_names(&self) -> &[&str] {
        &["update"]
    }

    async fn execute(&self, collection: Collection) -> Result<()> {
        let options = self.upsert.map(|b| UpdateOptions {
            upsert: Some(b),
            ..Default::default()
        });
        collection
            .update_one(self.filter.clone(), self.update.clone(), options)
            .await
            .map(|_| ())
    }
}

impl EventClient {
    pub(super) async fn run_operation_with_events(
        &self,
        operation: AnyTestOperation,
        database_name: &str,
        collection_name: &str,
    ) -> Vec<CommandEvent> {
        let _: Result<_> = operation
            .execute(self.database(database_name).collection(collection_name))
            .await;
        self.get_command_events(operation.command_names())
    }
}
