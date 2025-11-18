pub mod bson_decode;
pub mod bson_encode;
pub mod bulk_write;
pub mod find_many;
pub mod find_one;
pub mod gridfs_download;
pub mod gridfs_multi_download;
pub mod gridfs_multi_upload;
pub mod gridfs_upload;
pub mod insert_many;
pub mod insert_one;
pub mod json_multi_export;
pub mod json_multi_import;
pub mod run_command;

use std::{
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use futures::stream::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::{
    bson::{doc, Bson, Document},
    options::{Acknowledgment, ClientOptions, SelectionCriteria, WriteConcern},
    Client,
};
use serde_json::Value;
use std::sync::LazyLock;

use crate::fs::{BufReader, File};

static DATABASE_NAME: LazyLock<String> = LazyLock::new(|| {
    option_env!("DATABASE_NAME")
        .unwrap_or("perftest")
        .to_string()
});
static COLL_NAME: LazyLock<String> =
    LazyLock::new(|| option_env!("COLL_NAME").unwrap_or("corpus").to_string());
static MAX_EXECUTION_TIME: LazyLock<u64> = LazyLock::new(|| {
    option_env!("MAX_EXECUTION_TIME")
        .unwrap_or("300")
        .parse::<u64>()
        .expect("invalid MAX_EXECUTION_TIME")
});
static MIN_EXECUTION_TIME: LazyLock<u64> = LazyLock::new(|| {
    option_env!("MIN_EXECUTION_TIME")
        .unwrap_or("60")
        .parse::<u64>()
        .expect("invalid MIN_EXECUTION_TIME")
});
pub static TARGET_ITERATION_COUNT: LazyLock<usize> = LazyLock::new(|| {
    option_env!("TARGET_ITERATION_COUNT")
        .unwrap_or("100")
        .parse::<usize>()
        .expect("invalid TARGET_ITERATION_COUNT")
});

#[async_trait::async_trait]
pub trait Benchmark: Sized {
    /// The options used to construct the benchmark.
    type Options;
    /// The state needed to perform the benchmark task.
    type TaskState: Default;

    /// execute once before benchmarking
    async fn setup(options: Self::Options) -> Result<Self>;

    /// execute at the beginning of every iteration
    async fn before_task(&self) -> Result<Self::TaskState> {
        Ok(Default::default())
    }

    async fn do_task(&self, state: Self::TaskState) -> Result<()>;

    /// execute at the end of every iteration
    async fn after_task(&self) -> Result<()> {
        Ok(())
    }

    /// execute once after benchmarking
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
    elapsed >= *MAX_EXECUTION_TIME
        || (iter >= *TARGET_ITERATION_COUNT && elapsed > *MIN_EXECUTION_TIME)
}

pub async fn run_benchmark<B: Benchmark + Send + Sync>(
    options: B::Options,
) -> Result<Vec<Duration>> {
    let test = B::setup(options).await?;

    let mut test_durations = Vec::new();

    let progress_bar = ProgressBar::new(*TARGET_ITERATION_COUNT as u64);
    progress_bar.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos:>2}/{len:2} \
                 ({eta})",
            )?
            .progress_chars("#>-"),
    );

    let benchmark_timer = Instant::now();
    let mut iter = 0;
    while !finished(benchmark_timer.elapsed(), iter) {
        progress_bar.inc(1);

        let state = test.before_task().await?;
        let timer = Instant::now();
        test.do_task(state).await?;
        test_durations.push(timer.elapsed());
        test.after_task().await?;

        iter += 1;
    }
    test.teardown().await?;
    progress_bar.finish();

    test_durations.sort();
    Ok(test_durations)
}

pub async fn drop_database(uri: &str, database: &str) -> Result<()> {
    let mut options = ClientOptions::parse(uri).await?;
    options.write_concern = Some(WriteConcern::builder().w(Acknowledgment::Majority).build());
    let client = Client::with_options(options.clone())?;

    let hello = client
        .database("admin")
        .run_command(doc! { "hello": true })
        .await?;

    client.database(database).drop().await?;

    // in sharded clusters, take additional steps to ensure database is dropped completely.
    // see: https://www.mongodb.com/docs/manual/reference/method/db.dropDatabase/#replica-set-and-sharded-clusters
    let is_sharded = hello.get_str("msg").ok() == Some("isdbgrid");
    if is_sharded {
        client.database(database).drop().await?;
        for host in options.hosts {
            client
                .database("admin")
                .run_command(doc! { "flushRouterConfig": 1 })
                .selection_criteria(SelectionCriteria::Predicate(Arc::new(move |s| {
                    s.address() == &host
                })))
                .await?;
        }
    }

    Ok(())
}
