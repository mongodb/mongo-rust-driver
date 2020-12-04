use std::path::PathBuf;

use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt};
use mongodb::{options::InsertManyOptions, Client, Collection, Database};

use crate::{
    bench::{parse_json_file_to_documents, Benchmark, COLL_NAME, DATABASE_NAME},
    fs::File,
};

const TOTAL_FILES: usize = 100;

pub struct JsonMultiImportBenchmark {
    db: Database,
    coll: Collection,
    path: PathBuf,
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
        db.drop(None).await?;

        let coll = db.collection(&COLL_NAME);

        Ok(JsonMultiImportBenchmark {
            db,
            coll,
            path: options.path,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop(None).await?;
        self.db.create_collection(&COLL_NAME, None).await?;

        Ok(())
    }

    async fn do_task(&self) -> Result<()> {
        let mut tasks = FuturesUnordered::new();

        for i in 0..TOTAL_FILES {
            let coll_ref = self.coll.clone();
            let path = self.path.clone();

            tasks.push(async move {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a lot
                // of work for little gain since we `unwrap()` in main.rs anyway.
                let mut docs = Vec::new();

                let json_file_name = path.join(format!("ldjson{:03}.txt", i));
                let file = File::open_read(&json_file_name).await.unwrap();

                let mut new_docs = parse_json_file_to_documents(file).await.unwrap();

                docs.append(&mut new_docs);

                let opts = Some(InsertManyOptions::builder().ordered(Some(false)).build());
                coll_ref.insert_many(docs, opts).await.unwrap();
            });
        }

        while !tasks.is_empty() {
            tasks.next().await;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        self.db.drop(None).await?;

        Ok(())
    }
}
