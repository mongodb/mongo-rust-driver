use std::{fs::File, path::PathBuf};

use bson::{Bson, Document};
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::{Benchmark, COLL_NAME, DATABASE_NAME},
    error::{Error, Result},
};

pub struct InsertManyBenchmark {
    db: Database,
    num_copies: usize,
    coll: Collection,
    doc: Document,
}

// Specifies the options to a `InsertManyBenchmark::setup` operation.
pub struct Options {
    pub num_copies: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for InsertManyBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database(&DATABASE_NAME);
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;

        let coll = db.collection(&COLL_NAME);

        Ok(InsertManyBenchmark {
            db,
            num_copies: options.num_copies,
            coll,
            doc: match json.into() {
                Bson::Document(doc) => doc,
                _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
            },
        })
    }

    fn before_task(&mut self) -> Result<()> {
        self.coll.drop()?;
        self.db.create_collection(&COLL_NAME, None)?;

        Ok(())
    }

    fn do_task(&self) -> Result<()> {
        let insertions = vec![self.doc.clone(); self.num_copies];
        self.coll.insert_many(insertions, None)?;

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
