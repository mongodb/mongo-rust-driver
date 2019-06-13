use std::path::PathBuf;

use bson::Document;
use mongodb::{Client, Database};

use crate::{bench::Benchmark, error::Result};

pub struct RunCommandBenchmark {
    db: Database,
    num_iter: i32,
    cmd: Document,
}

impl Benchmark for RunCommandBenchmark {
    fn setup(num_iter: i32, _path: Option<PathBuf>, uri: Option<&str>) -> Result<Self> {
        let client = Client::with_uri_str(uri.unwrap_or("mongodb://localhost:27017"))?;
        let db = client.database("perftest");
        db.drop()?;

        Ok(RunCommandBenchmark {
            db,
            num_iter,
            cmd: doc! { "ismaster": true },
        })
    }

    fn do_task(&self) -> Result<()> {
        for _ in 0..self.num_iter {
            let _doc = self.db.run_command(self.cmd.clone(), None)?;
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
