use std::{fs::File, path::PathBuf};

use bson::Bson;
use mongodb::{options::FindOptions, Client, Collection, Database};
use serde_json::Value;

use crate::{
    bench::Benchmark,
    error::{Error, Result},
};

pub struct FindOneBenchmark {
    db: Database,
    num_iter: usize,
    coll: Collection,
}

// Specifies the options to a `FindOneBenchmark::setup` operation.
pub struct Options {
    pub num_iter: usize,
    pub path: PathBuf,
    pub uri: String,
}

impl Benchmark for FindOneBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database("perftest");
        db.drop()?;

        let mut file = File::open(options.path)?;

        let json: Value = serde_json::from_reader(&mut file)?;
        let mut doc = match json.into() {
            Bson::Document(doc) => doc,
            _ => return Err(Error::UnexpectedJson("invalid json test file".to_string())),
        };

        let coll = db.collection("corpus");
        for i in 0..options.num_iter {
            doc.insert("_id", i as i32);
            coll.insert_one(doc.clone(), None)?;
        }

        Ok(FindOneBenchmark {
            db,
            num_iter: options.num_iter,
            coll,
        })
    }

    fn do_task(&self) -> Result<()> {
        for i in 0..self.num_iter {
            let find_options = FindOptions::builder().limit(Some(1)).build();
            let mut cursor = self
                .coll
                .find(Some(doc! { "_id": i as i32 }), Some(find_options))?;
            let _doc = cursor.next();
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
