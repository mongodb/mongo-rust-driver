use std::{convert::TryInto, path::PathBuf};

use anyhow::{bail, Result};
use mongodb::{
    bson::{doc, Bson, Document},
    Client,
    Collection,
    Database,
};
use serde_json::Value;

use crate::{
    bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME},
    fs::read_to_string,
};

pub struct FindOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection<Document>,
    uri: String,
}

/// Specifies the options to a `FindOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for FindOneBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let num_iter = options.num_iter;

        let file = read_to_string(&options.path).await?;

        let json: Value = serde_json::from_str(&file)?;
        let mut doc = match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json test file"),
        };

        let coll = db.collection(&COLL_NAME);
        for i in 0..num_iter {
            doc.insert("_id", i as i32);
            coll.insert_one(doc.clone()).await?;
        }

        Ok(FindOneBenchmark {
            db,
            num_iter,
            coll,
            uri: options.uri,
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        for i in 0..self.num_iter {
            self.coll.find_one(doc! { "_id": i as i32 }).await?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
