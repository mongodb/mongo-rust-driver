use std::path::PathBuf;

use anyhow::Result;
use futures::stream::TryStreamExt;
use mongodb::{
    bson::{doc, RawDocumentBuf},
    Client,
    Collection,
    Database,
};

use crate::{
    bench::{parse_json_file_to_documents, Benchmark, COLL_NAME, DATABASE_NAME},
    fs::File,
};

use super::drop_database;

const TOTAL_FILES: usize = 100;

pub struct JsonMultiExportBenchmark {
    uri: String,
    db: Database,
    coll: Collection<RawDocumentBuf>,
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
        drop_database(&options.uri, &DATABASE_NAME).await?;

        let coll = db.collection(&COLL_NAME);

        let mut tasks = Vec::new();

        for i in 0..TOTAL_FILES {
            let path = options.path.clone();
            let coll = coll.clone();

            tasks.push(crate::spawn(async move {
                let json_file_name = path.join(format!("ldjson{:03}.txt", i));
                let file = File::open_read(&json_file_name).await?;

                let docs = parse_json_file_to_documents(file).await?;

                for mut doc in docs {
                    doc.insert("file", i as i32);
                    coll.insert_one(doc).await?;
                }

                let ok: anyhow::Result<()> = Ok(());
                ok
            }));
        }

        for task in tasks {
            task.await?;
        }

        Ok(JsonMultiExportBenchmark {
            uri: options.uri,
            db,
            coll: coll.clone_with_type(),
        })
    }

    async fn do_task(&mut self) -> Result<()> {
        let mut tasks = Vec::new();

        for i in 0..TOTAL_FILES {
            let coll_ref = self.coll.clone();
            let path = std::env::temp_dir();

            tasks.push(crate::spawn(async move {
                // Note that errors are unwrapped within threads instead of propagated with `?`.
                // While we could set up a channel to send errors back to main thread, this is a
                // lot of work for little gain since we `unwrap()` in
                // main.rs anyway.
                let file_name = path.join(format!("ldjson{:03}.txt", i));
                let mut file = File::open_write(&file_name).await.unwrap();

                let mut cursor = coll_ref.find(doc! { "file": i as i32 }).await.unwrap();

                while let Some(doc) = cursor.try_next().await.unwrap() {
                    file.write_line(serde_json::to_string(&doc).unwrap().as_str())
                        .await
                        .unwrap();
                }

                file.flush().await.unwrap();
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
