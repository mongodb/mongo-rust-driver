use std::{fs::File, path::PathBuf};

use bson::Bson;
use mongodb::{Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::Benchmark,
    error::{Error, Result},
};

pub struct FindOneBenchmark {
    db: Database,
    num_iter: i32,
    coll: Collection,
}

impl Benchmark for FindOneBenchmark {
    fn setup(num_iter: i32, path: Option<PathBuf>, uri: Option<&str>) -> Result<Self> {
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
        let mut doc = match json.into() {
            Bson::Document(doc) => doc,
            _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
        };

        let coll = db.collection("corpus");
        for x in 0..10000 {
            doc.insert("_id", x);
            coll.insert_one(doc.clone(), None)?;
        }

        Ok(FindOneBenchmark { db, num_iter, coll })
    }

    fn do_task(&self) -> Result<()> {
        for x in 0..self.num_iter {
            let mut cursor = self.coll.find(Some(doc! { "_id": x }), None)?;
            let _doc = cursor.next();
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
