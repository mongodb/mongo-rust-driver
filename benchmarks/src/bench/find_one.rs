use anyhow::Result;
use mongodb::{
    bson::{doc, Document},
    Client,
    Collection,
    Database,
};

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct FindOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection<Document>,
    uri: String,
}

/// Specifies the options to a `FindOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub doc: Document,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for FindOneBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let coll = db.collection(&COLL_NAME);
        for i in 0..options.num_iter {
            let mut doc = options.doc.clone();
            doc.insert("_id", i as i32);
            coll.insert_one(doc).await?;
        }

        Ok(FindOneBenchmark {
            db,
            num_iter: options.num_iter,
            coll,
            uri: options.uri,
        })
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        for i in 0..self.num_iter {
            self.coll.find_one(doc! { "_id": i as i32 }).await?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name()).await?;

        Ok(())
    }
}
