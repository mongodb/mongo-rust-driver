pub mod bson_decode;
pub mod bson_encode;
pub mod find_many;
pub mod find_one;
pub mod insert_many;
pub mod insert_one;
pub mod json_multi_export;
pub mod json_multi_import;
pub mod run_command;

use std::{
    convert::TryInto,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use futures::stream::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use mongodb::bson::{Bson, Document};
use serde_json::Value;

use crate::fs::{BufReader, File};

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
    pub static ref TARGET_ITERATION_COUNT: usize = option_env!("TARGET_ITERATION_COUNT")
        .unwrap_or("100")
        .parse::<usize>()
        .expect("invalid TARGET_ITERATION_COUNT");
}

#[async_trait::async_trait]
pub trait Benchmark: Sized {
    type Options;

    // execute once before benchmarking
    async fn setup(options: Self::Options) -> Result<Self>;

    // execute at the beginning of every iteration
    async fn before_task(&mut self) -> Result<()> {
        Ok(())
    }

    async fn do_task(&self) -> Result<()>;

    // execute at the end of every iteration
    async fn after_task(&self) -> Result<()> {
        Ok(())
    }

    // execute once after benchmarking
    async fn teardown(&self) -> Result<()> {
        Ok(())
    }
}

pub(crate) async fn parse_json_file_to_documents(file: File) -> Result<Vec<Document>> {
    let mut docs: Vec<Document> = Vec::new();

    let mut lines = BufReader::new(file).lines();

    while let Some(line) = lines.try_next().await? {
        let json: Value = serde_json::from_str(&line)?;

        docs.push(match json.try_into()? {
            Bson::Document(doc) => doc,
            _ => bail!("invalid json document"),
        });
    }

    Ok(docs)
}

fn finished(duration: Duration, iter: usize) -> bool {
    let elapsed = duration.as_secs();
    elapsed >= *MAX_EXECUTION_TIME || (iter >= *TARGET_ITERATION_COUNT && elapsed > *MIN_EXECUTION_TIME)
}

pub async fn run_benchmark<B: Benchmark + Send + Sync>(
    options: B::Options,
) -> Result<Vec<Duration>> {
    let mut test = B::setup(options).await?;

    let mut test_durations = Vec::new();

    let progress_bar = ProgressBar::new(*TARGET_ITERATION_COUNT as u64);
    progress_bar.set_style(
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
        progress_bar.inc(1);

        test.before_task().await?;
        let timer = Instant::now();
        test.do_task().await?;
        test_durations.push(timer.elapsed());
        test.after_task().await?;

        iter += 1;
    }
    test.teardown().await?;
    progress_bar.finish();

    test_durations.sort();
    Ok(test_durations)
}
