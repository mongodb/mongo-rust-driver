use anyhow::Result;
use mongodb::{
    bson::{doc, Document},
    options::{DeleteOneModel, InsertOneModel, ReplaceOneModel, WriteModel},
    Client,
    Namespace,
};
use once_cell::sync::Lazy;

use super::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct InsertBulkWriteBenchmark {
    client: Client,
    uri: String,
    write_models: Vec<WriteModel>,
}

pub struct Options {
    pub uri: String,
    pub doc: Document,
    pub num_models: usize,
}

#[async_trait::async_trait]
impl Benchmark for InsertBulkWriteBenchmark {
    type Options = Options;
    type TaskState = Vec<WriteModel>;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let write_models = vec![
            WriteModel::InsertOne(
                InsertOneModel::builder()
                    .namespace(Namespace::new(DATABASE_NAME.as_str(), COLL_NAME.as_str()))
                    .document(options.doc.clone())
                    .build()
            );
            options.num_models
        ];

        Ok(Self {
            client,
            uri: options.uri,
            write_models,
        })
    }

    async fn before_task(&self) -> Result<Self::TaskState> {
        self.client
            .database(&DATABASE_NAME)
            .collection::<Document>(&COLL_NAME)
            .drop()
            .await?;
        self.client
            .database(&DATABASE_NAME)
            .create_collection(COLL_NAME.as_str())
            .await?;
        Ok(self.write_models.clone())
    }

    async fn do_task(&self, write_models: Self::TaskState) -> Result<()> {
        self.client.bulk_write(write_models).await?;
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), DATABASE_NAME.as_str()).await?;
        Ok(())
    }
}

static COLLECTION_NAMES: Lazy<Vec<String>> =
    Lazy::new(|| (1..=10).map(|i| format!("corpus_{}", i)).collect());

pub struct MixedBulkWriteBenchmark {
    client: Client,
    uri: String,
    write_models: Vec<WriteModel>,
}

#[async_trait::async_trait]
impl Benchmark for MixedBulkWriteBenchmark {
    type Options = Options;
    type TaskState = Vec<WriteModel>;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let mut write_models = Vec::new();
        for i in 0..options.num_models {
            let collection_name = COLLECTION_NAMES.get(i % 10).unwrap();
            let namespace = Namespace::new(DATABASE_NAME.as_str(), collection_name);
            if i % 3 == 0 {
                write_models.push(
                    InsertOneModel::builder()
                        .namespace(namespace)
                        .document(options.doc.clone())
                        .build()
                        .into(),
                );
            } else if i % 3 == 1 {
                write_models.push(
                    ReplaceOneModel::builder()
                        .namespace(namespace)
                        .filter(doc! {})
                        .replacement(options.doc.clone())
                        .build()
                        .into(),
                );
            } else {
                write_models.push(
                    DeleteOneModel::builder()
                        .namespace(namespace)
                        .filter(doc! {})
                        .build()
                        .into(),
                );
            }
        }

        Ok(Self {
            client,
            uri: options.uri,
            write_models,
        })
    }

    async fn before_task(&self) -> Result<Self::TaskState> {
        let database = self.client.database(&DATABASE_NAME);
        database.drop().await?;
        for collection_name in COLLECTION_NAMES.iter() {
            database.create_collection(collection_name).await?;
        }
        Ok(self.write_models.clone())
    }

    async fn do_task(&self, write_models: Self::TaskState) -> Result<()> {
        self.client.bulk_write(write_models).await?;
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), DATABASE_NAME.as_str()).await?;
        Ok(())
    }
}
