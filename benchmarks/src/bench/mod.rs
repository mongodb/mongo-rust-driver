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
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::Value;

use crate::error::{Error, Result};

lazy_static! {
    static ref DATABASE_NAME: String = option_env!("DATABASE_NAME")
        .unwrap_or("perftest")
        .to_string();
    static ref COLL_NAME: String = option_env!("COLL_NAME").unwrap_or("corpus").to_string();
    static ref MAX_EXECUTION_TIME: u64 = option_env!("MAX_EXECUTION_TIME")
        .unwrap_or("300")
        .parse::<u64>()
        .expect("invalid MAX_EXECUTION_TIME");
    static ref MIN_EXECUTION_TIME: u64 = option_env!("MIN_EXECUTION_TIME")
        .unwrap_or("60")
        .parse::<u64>()
        .expect("invalid MIN_EXECUTION_TIME");
    pub static ref MAX_ITERATIONS: usize = option_env!("MAX_ITERATIONS")
        .unwrap_or("100")
        .parse::<usize>()
        .expect("invalid MAX_ITERATIONS");
}

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
    elapsed >= *MAX_EXECUTION_TIME || (iter >= *MAX_ITERATIONS && elapsed > *MIN_EXECUTION_TIME)
}

pub fn run_benchmark<B: Benchmark>(options: B::Options) -> Result<Vec<Duration>> {
    let mut test = B::setup(options)?;

    let mut test_durations = Vec::new();

    let bar = ProgressBar::new(*MAX_ITERATIONS as u64);
    bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>2}/{len:2} \
                 ({eta})",
            )
            .progress_chars("#>-"),
    );

    let benchmark_timer = Instant::now();
    let mut iter = 0;
    while !finished(benchmark_timer.elapsed(), iter) {
        bar.inc(1);

        test.before_task()?;
        let timer = Instant::now();
        test.do_task()?;
        test_durations.push(timer.elapsed());
        test.after_task()?;

        iter += 1;
    }
    test.teardown()?;
    bar.finish();

    test_durations.sort();
    Ok(test_durations)
}
