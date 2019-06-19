#[macro_use]
extern crate bson;
#[macro_use]
extern crate lazy_static;
extern crate num_cpus;

mod bench;
mod error;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{bench::Benchmark, error::Result};

lazy_static! {
    static ref DATA_PATH: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("data");
}

fn score_test(durations: Vec<Duration>, name: &str, task_size: f64, more_info: bool) -> f64 {
    let dur_len: f64 = durations.len() as f64;

    let score: f64 =
        durations[(dur_len * 50.0 / 100.0) as usize - 1].as_millis() as f64 / task_size;

    println!("TEST: {} -- Score: {}", name, score);
    if more_info {
        println!(
            "10th percentile: {:#?}",
            durations[(dur_len * 10.0 / 100.0) as usize - 1]
        );
        println!(
            "25th percentile: {:#?}",
            durations[(dur_len * 25.0 / 100.0) as usize - 1]
        );
        println!(
            "50th percentile: {:#?}",
            durations[(dur_len * 50.0 / 100.0) as usize - 1]
        );
        println!(
            "75th percentile: {:#?}",
            durations[(dur_len * 75.0 / 100.0) as usize - 1]
        );
        println!(
            "90th percentile: {:#?}",
            durations[(dur_len * 90.0 / 100.0) as usize - 1]
        );
        println!(
            "95th percentile: {:#?}",
            durations[(dur_len * 95.0 / 100.0) as usize - 1]
        );
        println!(
            "98th percentile: {:#?}",
            durations[(dur_len * 98.0 / 100.0) as usize - 1]
        );
        println!(
            "99th percentile: {:#?}",
            durations[(dur_len * 99.0 / 100.0) as usize - 1]
        );
    }

    score
}

fn single_doc_benchmarks(more_info: bool) -> Result<f64> {
    println!("Single-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Run command
    let run_command = bench::run_benchmark(bench::run_command::RunCommandBenchmark::setup(
        10000, None, None,
    )?)?;

    comp_score += score_test(run_command, "Run command", 0.16, more_info);

    // Find one by ID
    let find_one = bench::run_benchmark(bench::find_one::FindOneBenchmark::setup(
        10000,
        Some(DATA_PATH.join("single_and_multi_document/tweet.json")),
        None,
    )?)?;

    comp_score += score_test(find_one, "Find one by ID", 16.22, more_info);

    // Small doc insertOne
    let small_insert_one = bench::run_benchmark(bench::insert_one::InsertOneBenchmark::setup(
        10000,
        Some(DATA_PATH.join("single_and_multi_document/small_doc.json")),
        None,
    )?)?;

    comp_score += score_test(small_insert_one, "Small doc insertOne", 2.75, more_info);

    // Large doc insertOne
    let large_insert_one = bench::run_benchmark(bench::insert_one::InsertOneBenchmark::setup(
        10,
        Some(DATA_PATH.join("single_and_multi_document/large_doc.json")),
        None,
    )?)?;

    comp_score += score_test(large_insert_one, "Large doc insertOne", 27.31, more_info);

    Ok(comp_score)
}

fn multi_doc_benchmarks(more_info: bool) -> Result<f64> {
    println!("Multi-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Find many and empty the cursor
    let find_many = bench::run_benchmark(bench::find_many::FindManyBenchmark::setup(
        10000,
        Some(DATA_PATH.join("single_and_multi_document/tweet.json")),
        None,
    )?)?;

    comp_score += score_test(
        find_many,
        "Find many and empty the cursor",
        16.22,
        more_info,
    );

    // Small doc bulk insert
    let small_insert_many = bench::run_benchmark(bench::insert_many::InsertManyBenchmark::setup(
        10000,
        Some(DATA_PATH.join("single_and_multi_document/small_doc.json")),
        None,
    )?)?;

    comp_score += score_test(small_insert_many, "Small doc bulk insert", 2.75, more_info);

    // Large doc bulk insert
    let large_insert_many = bench::run_benchmark(bench::insert_many::InsertManyBenchmark::setup(
        10000,
        Some(DATA_PATH.join("single_and_multi_document/large_doc.json")),
        None,
    )?)?;

    comp_score += score_test(large_insert_many, "Large doc bulk insert", 27.31, more_info);

    Ok(comp_score)
}

fn parallel_benchmarks(more_info: bool) -> Result<f64> {
    println!("Parallel Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // LDJSON multi-file import
    let json_multi_import =
        bench::run_benchmark(bench::json_multi_import::JsonMultiImportBenchmark::setup(
            num_cpus::get() as i32,
            Some(DATA_PATH.join("parallel/ldjson_multi")),
            None,
        )?)?;

    comp_score += score_test(
        json_multi_import,
        "LDJSON multi-file import",
        565.0,
        more_info,
    );

    // LDJSON multi-file export
    let json_multi_export =
        bench::run_benchmark(bench::json_multi_export::JsonMultiExportBenchmark::setup(
            num_cpus::get() as i32,
            Some(DATA_PATH.join("parallel/ldjson_multi")),
            None,
        )?)?;

    comp_score += score_test(
        json_multi_export,
        "LDJSON multi-file export",
        565.0,
        more_info,
    );

    Ok(comp_score)
}

fn main() {
    println!("Running tests...");

    let mut comp_score: f64 = 0.0;

    let single_doc_score = single_doc_benchmarks(false).unwrap();
    let multi_doc_score = multi_doc_benchmarks(false).unwrap();
    let parallel_score = parallel_benchmarks(false).unwrap();

    comp_score += single_doc_score + multi_doc_score + parallel_score;

    println!("----------------------------");
    println!("COMPOSITE SCORE = {}", comp_score);
}
