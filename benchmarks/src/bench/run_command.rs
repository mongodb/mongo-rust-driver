use anyhow::{Context, Result};
use mongodb::{
    bson::{doc, Document},
    Client,
    Database,
};

use crate::bench::{drop_database, Benchmark, DATABASE_NAME};

pub struct RunCommandBenchmark {
    db: Database,
    num_iter: usize,
    cmd: Document,
    uri: String,
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
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        Ok(RunCommandBenchmark {
            db,
            num_iter: options.num_iter,
            cmd: doc! { "hello": true },
            uri: options.uri,
        })
    }

    async fn do_task(&self) -> Result<()> {
        for _ in 0..self.num_iter {
            let _doc = self
                .db
                .run_command(self.cmd.clone())
                .await
                .context("run command")?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
