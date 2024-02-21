use std::path::PathBuf;

use anyhow::Result;
use futures::stream::TryStreamExt;
use mongodb::{options::InsertManyOptions, Client, Collection, Database};

use crate::{
    bench::{Benchmark, COLL_NAME, DATABASE_NAME},
    fs::{BufReader, File},
    models::tweet::Tweet,
};

use super::drop_database;

const TOTAL_FILES: usize = 100;

pub struct JsonMultiImportBenchmark {
    db: Database,
    coll: Collection<Tweet>,
    path: PathBuf,
    uri: String,
}

// Specifies the options to a `JsonMultiImportBenchmark::setup` operation.
pub struct Options {
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for JsonMultiImportBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let coll = db.collection(&COLL_NAME);

        Ok(JsonMultiImportBenchmark {
            db,
            coll,
            path: options.path,
            uri: options.uri,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop().await?;
        self.db.create_collection(COLL_NAME.as_str(), None).await?;

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        let mut tasks = Vec::new();

        for i in 0..TOTAL_FILES {
            let coll_ref = self.coll.clone();
            let path = self.path.clone();

            tasks.push(crate::spawn(async move {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a lot
                // of work for little gain since we `unwrap()` in main.rs anyway.
                let mut docs: Vec<Tweet> = Vec::new();

                let json_file_name = path.join(format!("ldjson{:03}.txt", i));
                let file = File::open_read(&json_file_name).await.unwrap();

                let mut lines = BufReader::new(file).lines();
                while let Some(line) = lines.try_next().await.unwrap() {
                    docs.push(serde_json::from_str(&line).unwrap());
                }

                let opts = Some(InsertManyOptions::builder().ordered(false).build());
                coll_ref.insert_many(docs, opts).await.unwrap();
            }));
        }

        for task in tasks {
            task.await;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
