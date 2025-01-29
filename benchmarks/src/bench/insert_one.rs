use anyhow::{Context, Result};
use mongodb::{bson::Document, Client, Collection, Database};

use crate::bench::{drop_database, Benchmark, COLL_NAME, DATABASE_NAME};

pub struct InsertOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection<Document>,
    doc: Document,
    uri: String,
}

/// Specifies the options to a `InsertOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub doc: Document,
    pub uri: String,
}

#[async_trait::async_trait]
impl Benchmark for InsertOneBenchmark {
    type Options = Options;

    async fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri).await?;
        let db = client.database(&DATABASE_NAME);
        drop_database(&options.uri, &DATABASE_NAME).await?;

        let coll = db.collection(&COLL_NAME);

        Ok(InsertOneBenchmark {
            db,
            num_iter: options.num_iter,
            coll,
            doc: options.doc,
            uri: options.uri,
        })
    }

    async fn before_task(&mut self) -> Result<()> {
        self.coll.drop().await?;
        self.db
            .create_collection(COLL_NAME.as_str())
            .await
            .context("create collection")?;

        Ok(())
    }

    async fn do_task(&mut self) -> Result<()> {
        for _ in 0..self.num_iter {
            self.coll
                .insert_one(&self.doc)
                .await
                .context("insert one")?;
        }

        Ok(())
    }

    async fn teardown(&self) -> Result<()> {
        drop_database(&self.uri, &DATABASE_NAME)
            .await
            .context("drop database teardown")?;

        Ok(())
    }
}
