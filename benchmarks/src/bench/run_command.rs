use anyhow::Result;
use mongodb::{
    bson::{doc, Document},
    Client,
    Database,
};

use crate::bench::{Benchmark, DATABASE_NAME};

pub struct RunCommandBenchmark {
    db: Database,
    num_iter: usize,
    cmd: Document,
}

pub struct Options {
    pub num_iter: usize,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for RunCommandBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        Ok(RunCommandBenchmark {
            db,
            num_iter: options.num_iter,
            cmd: doc! { "ismaster": true },
        })
    }

    async fn do_task(&self) -> Result<()> {
        for _ in 0..self.num_iter {
            let _doc = self.db.run_command(self.cmd.clone(), None).await?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}
