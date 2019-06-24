use std::{fs::File, path::PathBuf};

use bson::{Bson, Document};
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::Benchmark,
    error::{Error, Result},
};

pub struct InsertManyBenchmark {
    db: Database,
    coll: Collection,
    doc: Document,
}

// Specifies the options to a `InsertManyBenchmark::setup` operation.
pub struct Options {
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for InsertManyBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database("perftest");
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;

        // We need to create a `Collection` in order to populate the field of the
        // InsertManyBenchmark being returned, so we create a placeholder that gets
        // overwritten in before_task().
        let coll = db.collection("placeholder");

        Ok(InsertManyBenchmark {
            db,
            coll,
            doc: match json.into() {
                Bson::Document(doc) => doc,
                _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
            },
        })
    }

    fn before_task(&mut self) -> Result<()> {
        self.coll = self.db.collection("corpus");
        self.coll.drop()?;

        Ok(())
    }

    fn do_task(&self) -> Result<()> {
        let insertions = vec![self.doc.clone(); 10000];
        self.coll.insert_many(insertions, None)?;

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
