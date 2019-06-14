pub mod find_many;
pub mod find_one;
pub mod insert_many;
pub mod insert_one;
pub mod json_multi_import;
pub mod run_command;

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    time::{Duration, Instant},
};

use bson::{Bson, Document};
use serde_json::Value;

use crate::error::{Error, Result};

pub trait Benchmark: Sized {
    // execute once before benchmarking
    fn setup(num_iter_or_threads: i32, path: Option<PathBuf>, uri: Option<&str>) -> Result<Self>;

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

fn finished(elapsed: u64, iter: usize) -> bool {
    elapsed >= 300 || (iter >= 99 && elapsed > 60)
}

pub fn run_benchmark(mut test: impl Benchmark) -> Result<Vec<Duration>> {
    let mut test_durations = Vec::new();

    let benchmark_timer = Instant::now();
    let mut iter = 0;
    while !finished(benchmark_timer.elapsed().as_secs(), iter) {
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
