use anyhow::Result;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Client, Database,
};

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct FindRawBatchesBenchmark {
    db: Database,
    uri: String,
}

/// Specifies the options to `FindRawBatchesBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub doc: Document,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for FindRawBatchesBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let coll = db.collection::<Document>(&COLL_NAME);
        let docs = vec![options.doc.clone(); options.num_iter];
        coll.insert_many(docs).await?;

        Ok(FindRawBatchesBenchmark {
            db,
            uri: options.uri,
        })
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        let mut batches = self.db.find_raw_batches(COLL_NAME.as_str(), doc! {}).await?;
        while let Some(batch_res) = batches.next().await {
            batch_res?;
        }
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;
        Ok(())
    }
}
