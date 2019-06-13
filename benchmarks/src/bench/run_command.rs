use bson::Document;
use mongodb::{Client, Database};

use crate::{bench::Benchmark, error::Result};

pub struct RunCommandBenchmark {
    db: Database,
    cmd: Document,
}

impl Benchmark for RunCommandBenchmark {
    type Context = ();

    fn setup(_path: Option<&str>, uri: Option<&str>) -> Result<Self> {
        let client = Client::with_uri_str(uri.unwrap_or("mongodb://localhost:27017"))?;
        let db = client.database("perftest");
        db.drop()?;

        Ok(RunCommandBenchmark {
            db,
            cmd: doc! { "ismaster": true },
        })
    }

    fn before_task(&self) -> Result<Self::Context> {
        Ok(())
    }

    fn do_task(&self, _context: Self::Context) -> Result<()> {
        for _ in 0..10000 {
            let _doc = self.db.run_command(self.cmd.clone(), None)?;
        }

        Ok(())
    }

    fn teardown(&self) -> Result<()> {
        self.db.drop()?;

        Ok(())
    }
}
