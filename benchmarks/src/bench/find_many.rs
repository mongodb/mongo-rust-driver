use std::{convert::TryInto, path::PathBuf};

use anyhow::{bail, Result};
use futures::stream::StreamExt;
use mongodb::{
    bson::{Bson, Document},
    Client,
    Collection,
    Database,
};
use serde_json::Value;

use crate::{
    bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME},
    fs::read_to_string,
};

pub struct FindManyBenchmark {
    db: Database,
    coll: Collection<Document>,
    uri: String,
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
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let num_iter = options.num_iter;

        let mut file = read_to_string(&options.path).await?;

        let json: Value = serde_json::from_str(&mut file)?;
        let doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let coll = db.collection(&COLL_NAME);
        let docs = vec![doc.clone(); num_iter];
        coll.insert_many(docs, None).await?;

        Ok(FindManyBenchmark {
            db,
            coll,
            uri: options.uri,
        })
    }

    async fn do_task(&self) -> Result<()> {
        let mut cursor = self.coll.find(None, None).await?;
        while let Some(doc) = cursor.next().await {
            doc?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
