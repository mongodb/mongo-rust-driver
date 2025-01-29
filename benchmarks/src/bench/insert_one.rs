use std::{convert::TryInto, fs::File, path::PathBuf};

use anyhow::{bail, Context, Result};
use mongodb::{
    bson::{Bson, Document},
    Client,
    Collection,
    Database,
};
use serde_json::Value;

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct InsertOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection<Document>,
    doc: Document,
    uri: String,
}

/// Specifies the options to a `InsertOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for InsertOneBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME).await?;

        let num_iter = options.num_iter;
        let uri = options.uri.clone();

        // This benchmark uses a file that's quite large, and unfortunately `serde_json` has no
        // async version of `from_reader`, so rather than read the whole file into memory at once,
        // we use the runtime's `spawn_blocking` functionality to do this efficiently.
        //
        // Note that the setup is _not_ measured as part of the benchmark runtime, so even if
        // `spawn_blocking` turned out not to be super efficient, it wouldn't be a big deal.
        let mut file = spawn_blocking_and_await!(File::open(options.path))?;
        let json: Value = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;

        let coll = db.collection(&COLL_NAME);

        Ok(InsertOneBenchmark {
            db,
            num_iter,
            coll,
            doc: match json.try_into()? {
                Bson::Document(doc) => doc,
                _ => bail!("invalid json test file"),
            },
            uri,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop().await?;
        self.db
            .create_collection(COLL_NAME.as_str())
            .await
            .context("create collection")?;

        Ok(())
    }

    async fn do_task(&mut self) -> Result<()> {
        for _ in 0..self.num_iter {
            self.coll
                .insert_one(&self.doc)
                .await
                .context("insert one")?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
