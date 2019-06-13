#[macro_use]
extern crate bson;
#[macro_use]
extern crate lazy_static;

mod bench;
mod error;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use crate::bench::Benchmark;

const NUM_ITERATIONS: u32 = 1;

lazy_static! {
    static ref DATA_PATH: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("data");
}

fn score_test(durations: Vec<Duration>, name: &str, task_size: f64, more_info: bool) -> f64 {
    let score: f64 = durations[49].as_millis() as f64 / task_size;

    println!("TEST: {} -- Score: {}", name, score);
    if more_info {
        println!("10th percentile: {:#?}", durations[9]);
        println!("25th percentile: {:#?}", durations[24]);
        println!("50th percentile: {:#?}", durations[49]);
        println!("75th percentile: {:#?}", durations[74]);
        println!("90th percentile: {:#?}", durations[89]);
        println!("95th percentile: {:#?}", durations[94]);
        println!("98th percentile: {:#?}", durations[97]);
        println!("99th percentile: {:#?}", durations[98]);
    }

    score
}

fn run_tests(more_info: bool) {
    let mut comp_score: f64 = 0.0;

    println!("Single-Doc Benchmarks:");
    println!("----------------------------");

    // Run command
    let run_command =
        bench::run_benchmark(bench::run_command::RunCommandBenchmark::setup(None, None).unwrap())
            .unwrap();

    comp_score += score_test(run_command, "Run command", 0.16, more_info);

    // Find one by ID
    let find_one = bench::run_benchmark(
        bench::find_one::FindOneBenchmark::setup(
            Some(DATA_PATH.join("single_and_multi_document/tweet.json")),
            None,
        )
        .unwrap(),
    )
    .unwrap();

    comp_score += score_test(find_one, "Find one by ID", 16.22, more_info);

    // Small doc insertOne
    let small_insert_one = bench::run_benchmark(
        bench::insert_one::InsertOneBenchmark::setup(
            Some(DATA_PATH.join("single_and_multi_document/small_doc.json")),
            None,
        )
        .unwrap(),
    )
    .unwrap();

    comp_score += score_test(small_insert_one, "Small doc insertOne", 2.75, more_info);

    // Large doc insertOne
    let large_insert_one = bench::run_benchmark(
        bench::insert_one::InsertOneBenchmark::setup(
            Some(DATA_PATH.join("single_and_multi_document/large_doc.json")),
            None,
        )
        .unwrap(),
    )
    .unwrap();

    comp_score += score_test(large_insert_one, "Large doc insertOne", 27.31, more_info);

    println!("----------------------------");
    println!("COMPOSITE SCORE = {}", comp_score);
}

fn main() {
    println!("Running tests...");
    run_tests(false);
}
