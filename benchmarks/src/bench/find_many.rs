use std::{convert::TryInto, path::PathBuf};

use anyhow::{bail, Result};
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Bson, Document, RawDocumentBuf},
    Client,
    Collection,
    Database,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{
    bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME},
    fs::read_to_string,
    models::tweet::Tweet,
};

pub struct FindManyBenchmark {
    db: Database,
    coll: Collection<RawDocumentBuf>,
    uri: String,
    mode: Mode,
}

// Specifies the options to `FindManyBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
    pub mode: Mode,
}

pub enum Mode {
    Document,
    RawBson,
    Serde,
}

#[async_trait::async_trait]
impl Benchmark for FindManyBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let num_iter = options.num_iter;

        let file = read_to_string(&options.path).await?;

        let json: Value = serde_json::from_str(&file)?;
        let doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let coll = db.collection(&COLL_NAME);
        let docs = vec![doc.clone(); num_iter];
        coll.insert_many(docs).await?;

        Ok(FindManyBenchmark {
            db,
            coll: coll.clone_with_type(),
            uri: options.uri,
            mode: options.mode,
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        async fn execute<T: DeserializeOwned + Unpin + Send + Sync + std::fmt::Debug>(
            bench: &FindManyBenchmark,
        ) -> Result<()> {
            let coll = bench.coll.clone_with_type::<T>();
            let mut cursor = coll.find(doc! {}).await?;
            while let Some(doc) = cursor.next().await {
                doc?;
            }
            Ok(())
        }

        match self.mode {
            Mode::Document => {
                execute::<Document>(self).await?;
            }
            Mode::RawBson => {
                execute::<RawDocumentBuf>(self).await?;
            }
            Mode::Serde => {
                execute::<Tweet>(self).await?;
            }
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
