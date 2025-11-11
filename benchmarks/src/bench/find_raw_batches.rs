use anyhow::Result;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Client,
    Collection,
    Database,
};

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct FindRawBatchesBenchmark {
    db: Database,
    coll: Collection<Document>,
    uri: String,
    num_iter: usize,
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

        let coll = db.collection(&COLL_NAME);
        let docs = vec![options.doc.clone(); options.num_iter];
        coll.insert_many(docs).await?;

        Ok(FindRawBatchesBenchmark {
            db,
            coll,
            uri: options.uri,
            num_iter: options.num_iter,
        })
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        // Drain the cursor using raw server batches.
        let mut batches = self.coll.find_raw_batches(doc! {}).await?;
        while let Some(batch_res) = batches.next().await {
            let batch = batch_res?;
            // Touch each element minimally to avoid full materialization.
            for elem in batch.doc_slices()?.into_iter() {
                let _ = elem?;
            }
        }
        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;
        Ok(())
    }
}


