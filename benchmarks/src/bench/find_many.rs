use std::{convert::TryInto, fs::File, path::PathBuf};

use anyhow::{bail, Result};
use futures::stream::StreamExt;
use mongodb::{bson::Bson, Client, Collection, Database};
use serde_json::Value;

use crate::bench::{Benchmark, COLL_NAME, DATABASE_NAME};

pub struct FindManyBenchmark {
    db: Database,
    coll: Collection,
}

// Specifies the options to `FindManyBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for FindManyBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        let num_iter = options.num_iter;

        let mut file = spawn_blocking_and_await!(File::open(options.path))?;

        let json: Value = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;
        let doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let coll = db.collection(&COLL_NAME);
        let docs = vec![doc.clone(); num_iter];
        coll.insert_many(docs, None).await?;

        Ok(FindManyBenchmark { db, coll })
    }

    async fn do_task(&self) -> Result<()> {
        let mut cursor = self.coll.find(None, None).await?;
        while let Some(doc) = cursor.next().await {
            doc?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}
