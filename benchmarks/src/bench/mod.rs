pub mod find_many;
pub mod find_one;
pub mod insert_many;
pub mod insert_one;
pub mod json_multi_export;
pub mod json_multi_import;
pub mod run_command;

use std::{
    fs::File,
    io::{BufRead, BufReader},
    time::{Duration, Instant},
};

use bson::{Bson, Document};
use serde_json::Value;

use crate::error::{Error, Result};

lazy_static! {
    static ref DATABASE_NAME: String = option_env!("DATABASE_NAME")
        .unwrap_or("perftest")
        .to_string();
    static ref COLL_NAME: String = option_env!("COLL_NAME").unwrap_or("corpus").to_string();
}

const MAX_EXECUTION_TIME: u64 = 300;
const MIN_EXECUTION_TIME: u64 = 60;
const MAX_ITERATIONS: usize = 99;

pub trait Benchmark: Sized {
    type Options;

    // execute once before benchmarking
    fn setup(options: Self::Options) -> Result<Self>;

    // execute at the beginning of every iteration
    fn before_task(&mut self) -> Result<()> {
        Ok(())
    }

    fn do_task(&self) -> Result<()>;

    // execute at the end of every iteration
    fn after_task(&self) -> Result<()> {
        Ok(())
    }

    // execute once after benchmarking
    fn teardown(&self) -> Result<()>;
}

pub fn parse_json_file_to_documents(file: File) -> Result<Vec<Document>> {
    let mut docs: Vec<Document> = Vec::new();

    for line in BufReader::new(file).lines() {
        let json: Value = serde_json::from_str(&mut line?)?;

        docs.push(match json.into() {
            Bson::Document(doc) => doc,
            _ => return Err(Error::UnexpectedJson("invalid json document".to_string())),
        });
    }

    Ok(docs)
}

fn finished(duration: Duration, iter: usize) -> bool {
    let elapsed = duration.as_secs();
    elapsed >= MAX_EXECUTION_TIME || (iter >= MAX_ITERATIONS && elapsed > MIN_EXECUTION_TIME)
}

pub fn run_benchmark(mut test: impl Benchmark) -> Result<Vec<Duration>> {
    let mut test_durations = Vec::new();

    let benchmark_timer = Instant::now();
    let mut iter = 0;
    while !finished(benchmark_timer.elapsed(), iter) {
        let timer = Instant::now();
        test.before_task()?;
        test.do_task()?;
        test.after_task()?;
        test_durations.push(timer.elapsed());

        iter += 1;
    }
    test.teardown()?;

    test_durations.sort();
    Ok(test_durations)
}
