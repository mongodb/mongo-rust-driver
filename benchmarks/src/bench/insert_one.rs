use std::{fs::File, path::PathBuf};

use bson::{Bson, Document};
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::{Benchmark, COLL_NAME, DATABASE_NAME},
    error::{Error, Result},
};

pub struct InsertOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection,
    doc: Document,
}

// Specifies the options to a `InsertOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for InsertOneBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database(&DATABASE_NAME);
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;

        let coll = db.collection(&COLL_NAME);
        coll.drop()?;
        db.create_collection(&COLL_NAME, None)?;

        Ok(InsertOneBenchmark {
            db,
            num_iter: options.num_iter,
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
        for _ in 0..self.num_iter {
            self.coll.insert_one(self.doc.clone(), None)?;
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
