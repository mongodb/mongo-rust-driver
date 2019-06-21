use std::{fs::File, path::PathBuf};

use bson::{Bson, Document};
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::Benchmark,
    error::{Error, Result},
};

pub struct InsertOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection,
    doc: Document,
}

impl Benchmark for InsertOneBenchmark {
    fn setup(num_iter: usize, path: Option<PathBuf>, uri: Option<&str>) -> Result<Self> {
        let client = Client::with_uri_str(uri.unwrap_or("mongodb://localhost:27017"))?;
        let db = client.database("perftest");
        db.drop()?;

        let mut file = File::open(match path {
            Some(path) => path,
            None => {
                return Err(Error::UnexpectedJson(
                    "invalid json test file path".to_string(),
                ))
            }
        })?;

        let json: Value = serde_json::from_reader(&mut file)?;

        // We need to create a collection in order to populate the field of the InsertManyBenchmark
        // being returned, so we create a placeholder that gets overwritten in before_task().
        let coll = db.collection("placeholder");

        Ok(InsertOneBenchmark {
            db,
            num_iter,
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
