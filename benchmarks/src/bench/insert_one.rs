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
    doc: Document,
}

impl Benchmark for InsertOneBenchmark {
    type Context = Collection;

    fn setup(path: Option<PathBuf>, uri: Option<&str>) -> Result<Self> {
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

        Ok(InsertOneBenchmark {
            db,
            doc: match json.into() {
                Bson::Document(doc) => doc,
                _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
            },
        })
    }

    fn before_task(&self) -> Result<Self::Context> {
        let coll = self.db.collection("corpus");
        coll.drop()?;

        Ok(coll)
    }

    fn do_task(&self, coll: Self::Context) -> Result<()> {
        for _ in 0..crate::NUM_ITERATIONS {
            coll.insert_one(self.doc.clone(), None)?;
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
