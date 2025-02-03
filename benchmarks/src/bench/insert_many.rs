use anyhow::{Context, Result};
use mongodb::{bson::Document, Client, Collection, Database};

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct InsertManyBenchmark {
    db: Database,
    num_copies: usize,
    coll: Collection<Document>,
    doc: Document,
    uri: String,
}

/// Specifies the options to a `InsertManyBenchmark::setup` operation.
pub struct Options {
    pub num_copies: usize,
    pub doc: Document,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for InsertManyBenchmark {
    type Options = Options;
    type TaskState = ();

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(options.uri.as_str(), DATABASE_NAME.as_str()).await?;

        let coll = db.collection(COLL_NAME.as_str());

        Ok(InsertManyBenchmark {
            db,
            num_copies: options.num_copies,
            coll,
            doc: options.doc,
            uri: options.uri,
        })
    }

    async fn before_task(&self) -> Result<Self::TaskState> {
        self.coll.drop().await?;
        self.db
            .create_collection(COLL_NAME.as_str())
            .await
            .context("create in before")?;

        Ok(())
    }

    async fn do_task(&self, _state: Self::TaskState) -> Result<()> {
        let insertions = vec![&self.doc; self.num_copies];
        self.coll
            .insert_many(insertions)
            .await
            .context("insert many")?;

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(self.uri.as_str(), self.db.name())
            .await
            .context("teardown")?;

        Ok(())
    }
}
