use bson::Document;
use mongodb::{Client, Database};

use crate::{
    bench::{Benchmark, DATABASE_NAME},
    error::Result,
};

pub struct RunCommandBenchmark {
    db: Database,
    num_iter: usize,
    cmd: Document,
}

pub struct Options {
    pub num_iter: usize,
    pub uri: String,
}

impl Benchmark for RunCommandBenchmark {
    type Options = Options;

    fn setup(options: Self::Options) -> Result<Self> {
        println!("Running RunCommandBenchmark...");

        let client = Client::with_uri_str(&options.uri)?;
        let db = client.database(&DATABASE_NAME);
        db.drop()?;

        Ok(RunCommandBenchmark {
            db,
            num_iter: options.num_iter,
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
