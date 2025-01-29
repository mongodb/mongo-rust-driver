use std::{fs::File, path::PathBuf};

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
    // create a fresh copy of the write models in before_task to avoid any noise from cloning a
    // large list of write models in do_task
    write_models_copy: Option<Vec<WriteModel>>,
}

pub struct Options {
    pub data_path: PathBuf,
    pub uri: String,
    pub num_models: usize,
}

#[async_trait::async_trait]
impl Benchmark for InsertBulkWriteBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let data_path = options.data_path.clone();
        let mut file = spawn_blocking_and_await!(File::open(data_path))?;
        let document: Document = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;

        let write_models = vec![
            WriteModel::InsertOne(
                InsertOneModel::builder()
                    .namespace(Namespace::new(DATABASE_NAME.as_str(), COLL_NAME.as_str()))
                    .document(document.clone())
                    .build()
            );
            options.num_models
        ];

        Ok(Self {
            client,
            uri: options.uri,
            write_models,
            write_models_copy: None,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.client
            .database(&DATABASE_NAME)
            .collection::<Document>(&COLL_NAME)
            .drop()
            .await?;
        self.client
            .database(&DATABASE_NAME)
            .create_collection(COLL_NAME.as_str())
            .await?;
        self.write_models_copy = Some(self.write_models.clone());
        Ok(())
    }

    async fn do_task(&mut self) -> Result<()> {
        let write_models = self.write_models_copy.take().unwrap();
        self.client.bulk_write(write_models).await?;
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), DATABASE_NAME.as_str()).await?;
        Ok(())
    }
}

static COLLECTION_NAMES: Lazy<Vec<String>> = Lazy::new(|| {
    (1..=10)
        .into_iter()
        .map(|i| format!("corpus_{}", i))
        .collect()
});

pub struct MixedBulkWriteBenchmark {
    client: Client,
    uri: String,
    write_models: Vec<WriteModel>,
    write_models_copy: Option<Vec<WriteModel>>,
}

#[async_trait::async_trait]
impl Benchmark for MixedBulkWriteBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let num_models = options.num_models;
        let uri = options.uri.clone();

        let mut file = spawn_blocking_and_await!(File::open(options.data_path))?;
        let document: Document = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;

        let mut write_models = Vec::new();
        for i in 0..num_models {
            let collection_name = COLLECTION_NAMES.get(i % 10).unwrap();
            let namespace = Namespace::new(DATABASE_NAME.as_str(), collection_name);
            if i % 3 == 0 {
                write_models.push(
                    InsertOneModel::builder()
                        .namespace(namespace)
                        .document(document.clone())
                        .build()
                        .into(),
                );
            } else if i % 3 == 1 {
                write_models.push(
                    ReplaceOneModel::builder()
                        .namespace(namespace)
                        .filter(doc! {})
                        .replacement(document.clone())
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
            uri,
            write_models,
            write_models_copy: None,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        let database = self.client.database(&DATABASE_NAME);
        database.drop().await?;
        for collection_name in COLLECTION_NAMES.iter() {
            database.create_collection(collection_name).await?;
        }
        self.write_models_copy = Some(self.write_models.clone());
        Ok(())
    }

    async fn do_task(&mut self) -> Result<()> {
        let write_models = self.write_models_copy.take().unwrap();
        self.client.bulk_write(write_models).await?;
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), DATABASE_NAME.as_str()).await?;
        Ok(())
    }
}
