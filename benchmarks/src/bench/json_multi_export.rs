use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

use anyhow::Result;
use futures::stream::{FuturesUnordered, StreamExt, TryStreamExt};
use mongodb::{bson::doc, Client, Collection, Database};

use crate::bench::{parse_json_file_to_documents, Benchmark, COLL_NAME, DATABASE_NAME};

const TOTAL_FILES: usize = 100;

pub struct JsonMultiExportBenchmark {
    db: Database,
    coll: Collection,
}

// Specifies the options to a `JsonMultiExportBenchmark::setup` operation.
pub struct Options {
    pub path: PathBuf,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for JsonMultiExportBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        db.drop(None).await?;

        let coll = db.collection(&COLL_NAME);

        let mut tasks = FuturesUnordered::new();

        for i in 0..TOTAL_FILES {
            let path = options.path.clone();
            let coll = coll.clone();

            tasks.push(async move {
                let json_file_name = path.join(format!("ldjson{:03}.txt", i));
                let file = spawn_blocking_and_await!(File::open(&json_file_name))?;

                let docs = spawn_blocking_and_await!(parse_json_file_to_documents(file))?;

                for mut doc in docs {
                    doc.insert("file", i as i32);
                    coll.insert_one(doc, None).await?;
                }

                let ok: anyhow::Result<()> = Ok(());
                ok
            });
        }

        while let Some(result) = tasks.next().await {
            println!("done!");
            result?;
        }

        Ok(JsonMultiExportBenchmark { db, coll })
    }

    async fn do_task(&self) -> Result<()> {
        let mut tasks = FuturesUnordered::new();

        for i in 0..TOTAL_FILES {
            let coll_ref = self.coll.clone();
            let path = std::env::temp_dir();

            tasks.push(async move {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a
                // lot of work for little gain since we `unwrap()` in
                // main.rs anyway.
                let file_name = path.join(format!("ldjson{:03}.txt", i));
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&file_name)
                    .unwrap();

                let mut cursor = coll_ref
                    .find(Some(doc! { "file": i as i32 }), None)
                    .await
                    .unwrap();

                let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

                let send_future = spawn!(async move {
                    while let Some(doc) = cursor.try_next().await.unwrap() {
                        sender.send(doc.to_string()).unwrap();
                    }
                });

                let rec_future = spawn_blocking_and_await!(async move {
                    while let Some(s) = receiver.next().await {
                        writeln!(file, "{}", s).unwrap();
                    }
                });

                futures::future::join(send_future, rec_future).await
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
