use std::{fs::File, path::PathBuf};

use bson::Bson;
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::{Benchmark, COLL_NAME, DATABASE_NAME},
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
        let db = client.database(&DATABASE_NAME);
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;
        let doc = match json.into() {
            Bson::Document(doc) => doc,
            _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
        };

        let coll = db.collection(&COLL_NAME);
        let docs = vec![doc.clone(); options.num_iter];
        coll.insert_many(docs, None)?;

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
