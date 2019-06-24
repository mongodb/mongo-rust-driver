use std::{fs::File, path::PathBuf};

use bson::Bson;
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::Benchmark,
    error::{Error, Result},
};

pub struct FindManyBenchmark {
    db: Database,
    coll: Collection,
}

// Specifies the options to `FindManyBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for FindManyBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database("perftest");
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;
        let doc = match json.into() {
            Bson::Document(doc) => doc,
            _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
        };

        // TODO RUST-187: We can change this to a single `Collection::insert_many` once batching is
        // implemented in the driver.
        let coll = db.collection("corpus");
        for _ in 0..options.num_iter {
            coll.insert_one(doc.clone(), None)?;
        }

        Ok(FindManyBenchmark { db, coll })
    }

    fn do_task(&self) -> Result<()> {
        let cursor = self.coll.find(None, None)?;
        for doc in cursor {
            doc?;
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
