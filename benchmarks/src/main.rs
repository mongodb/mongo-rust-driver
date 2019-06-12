// #[macro_use]
// extern crate bson;

mod bench;
mod error;

use std::time::Duration;

use crate::bench::Benchmark;

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

fn run_tests() {
    let mut comp_score: f64 = 0.0;

    let small_insert_one_test =
        bench::run_benchmark(bench::small_insert_one::InsertOneBenchmark::setup().unwrap())
            .unwrap();

    comp_score += score_test(small_insert_one_test, "Small doc insertOne", 2.75, false);

    println!("----------------------------");
    println!("COMPOSITE SCORE = {}", comp_score);
}

fn main() {
    run_tests();
}
