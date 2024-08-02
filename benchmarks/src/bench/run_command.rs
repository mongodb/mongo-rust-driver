use anyhow::{Context, Result};
use mongodb::{
    bson::{doc, Document},
    Client,
    Database,
};

use crate::bench::{drop_database, Benchmark, DATABASE_NAME};

pub struct RunCommandBenchmark {
    db: Option<Database>,
    num_iter: usize,
    cmd: Document,
    uri: String,
}

pub struct Options {
    pub num_iter: usize,
    pub uri: String,
    pub cold_start: bool,
}

impl RunCommandBenchmark {
    async fn init_db(uri: &str) -> Result<Database> {
        let client = Client::with_uri_str(uri).await?;
        Ok(client.database(&DATABASE_NAME))
    }

    async fn get_db(&self) -> Result<Database> {
        match self.db.as_ref() {
            Some(db) => Ok(db.clone()),
            None => Self::init_db(&self.uri).await,
        }
    }
}

#[async_trait::async_trait]
impl Benchmark for RunCommandBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let db = if options.cold_start {
            None
        } else {
            Some(Self::init_db(&options.uri).await?)
        };

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
                .get_db()
                .await?
                .run_command(self.cmd.clone())
                .await
                .context("run command")?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), &DATABASE_NAME).await?;

        Ok(())
    }
}
