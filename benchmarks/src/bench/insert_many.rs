use std::{convert::TryInto, fs::File, path::PathBuf};

use anyhow::{bail, Result};
use mongodb::{
    bson::{Bson, Document},
    Client,
    Collection,
    Database,
};
use serde_json::Value;

use crate::bench::{Benchmark, COLL_NAME, DATABASE_NAME};

pub struct InsertManyBenchmark {
    db: Database,
    num_copies: usize,
    coll: Collection,
    doc: Document,
}

// Specifies the options to a `InsertManyBenchmark::setup` operation.
pub struct Options {
    pub num_copies: usize,
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for InsertManyBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        let num_copies = options.num_copies;

        // This benchmark uses a file that's quite large, and unfortunately `serde_json` has no
        // async version of `from_reader`, so rather than read the whole file into memory at once,
        // we use the runtime's `spawn_blocking` functionality to do this efficiently.
        //
        // Note that the setup is _not_ measured as part of the benchmark runtime, so even if
        // `spawn_blocking` turned out not to be super efficient, it wouldn't be a big deal.
        let mut file = spawn_blocking_and_await!(File::open(options.path))?;
        let json: Value = spawn_blocking_and_await!(serde_json::from_reader(&mut file))?;

        let coll = db.collection(&COLL_NAME);

        Ok(InsertManyBenchmark {
            db,
            num_copies,
            coll,
            doc: match json.try_into()? {
                Bson::Document(doc) => doc,
                _ => bail!("invalid json test file"),
            },
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop(None).await?;
        self.db.create_collection(&COLL_NAME, None).await?;

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        let insertions = vec![self.doc.clone(); self.num_copies];
        self.coll.insert_many(insertions, None).await?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}
