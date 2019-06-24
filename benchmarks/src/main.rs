#[macro_use]
extern crate bson;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate clap;
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

fn single_doc_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("Single-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Run command
    let run_command_options = bench::run_command::Options {
        num_iter: 10000,
        uri: uri.to_string(),
    };
    let run_command = bench::run_benchmark(bench::run_command::RunCommandBenchmark::setup(
        run_command_options,
    )?)?;

    comp_score += score_test(run_command, "Run command", 0.16, more_info);

    // Find one by ID
    let find_one_options = bench::find_one::Options {
        num_iter: 10000,
        path: DATA_PATH.join("single_and_multi_document/tweet.json"),
        uri: uri.to_string(),
    };
    let find_one =
        bench::run_benchmark(bench::find_one::FindOneBenchmark::setup(find_one_options)?)?;

    comp_score += score_test(find_one, "Find one by ID", 16.22, more_info);

    // Small doc insertOne
    let small_insert_one_options = bench::insert_one::Options {
        num_iter: 10000,
        path: DATA_PATH.join("single_and_multi_document/small_doc.json"),
        uri: uri.to_string(),
    };
    let small_insert_one = bench::run_benchmark(bench::insert_one::InsertOneBenchmark::setup(
        small_insert_one_options,
    )?)?;

    comp_score += score_test(small_insert_one, "Small doc insertOne", 2.75, more_info);

    // Large doc insertOne
    let large_insert_one_options = bench::insert_one::Options {
        num_iter: 10,
        path: DATA_PATH.join("single_and_multi_document/large_doc.json"),
        uri: uri.to_string(),
    };
    let large_insert_one = bench::run_benchmark(bench::insert_one::InsertOneBenchmark::setup(
        large_insert_one_options,
    )?)?;

    comp_score += score_test(large_insert_one, "Large doc insertOne", 27.31, more_info);

    Ok(comp_score)
}

fn multi_doc_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("Multi-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Find many and empty the cursor
    let find_many_options = bench::find_many::Options {
        num_iter: 10000,
        path: DATA_PATH.join("single_and_multi_document/tweet.json"),
        uri: uri.to_string(),
    };
    let find_many = bench::run_benchmark(bench::find_many::FindManyBenchmark::setup(
        find_many_options,
    )?)?;

    comp_score += score_test(
        find_many,
        "Find many and empty the cursor",
        16.22,
        more_info,
    );

    // Small doc bulk insert
    let small_insert_many_options = bench::insert_many::Options {
        path: DATA_PATH.join("single_and_multi_document/small_doc.json"),
        uri: uri.to_string(),
    };
    let small_insert_many = bench::run_benchmark(bench::insert_many::InsertManyBenchmark::setup(
        small_insert_many_options,
    )?)?;

    comp_score += score_test(small_insert_many, "Small doc bulk insert", 2.75, more_info);

    // Large doc bulk insert
    let large_insert_many_options = bench::insert_many::Options {
        path: DATA_PATH.join("single_and_multi_document/large_doc.json"),
        uri: uri.to_string(),
    };
    let large_insert_many = bench::run_benchmark(bench::insert_many::InsertManyBenchmark::setup(
        large_insert_many_options,
    )?)?;

    comp_score += score_test(large_insert_many, "Large doc bulk insert", 27.31, more_info);

    Ok(comp_score)
}

fn parallel_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("Parallel Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // LDJSON multi-file import
    let json_multi_import_options = bench::json_multi_import::Options {
        num_threads: num_cpus::get(),
        path: DATA_PATH.join("parallel/ldjson_multi"),
        uri: uri.to_string(),
    };
    let json_multi_import = bench::run_benchmark(
        bench::json_multi_import::JsonMultiImportBenchmark::setup(json_multi_import_options)?,
    )?;

    comp_score += score_test(
        json_multi_import,
        "LDJSON multi-file import",
        565.0,
        more_info,
    );

    // LDJSON multi-file export
    let json_multi_export_options = bench::json_multi_export::Options {
        num_threads: num_cpus::get(),
        path: DATA_PATH.join("parallel/ldjson_multi"),
        uri: uri.to_string(),
    };
    let json_multi_export = bench::run_benchmark(
        bench::json_multi_export::JsonMultiExportBenchmark::setup(json_multi_export_options)?,
    )?;

    comp_score += score_test(
        json_multi_export,
        "LDJSON multi-file export",
        565.0,
        more_info,
    );

    Ok(comp_score)
}

fn main() {
    let matches = clap_app!(RustDriverBenchmark =>
        (version: "1.0")
        (about: "Runs performance micro-benchmarks on Rust driver")
        (author: "benjirewis")
        (@arg single: -s --single ... "Run single document benchmarks")
        (@arg multi: -m --multi ... "Run multi document benchmarks")
        (@arg parallel: -p --parallel ... "Run parallel document benchmarks")
        (@arg verbose: -v --verbose ... "Print test information verbosely"))
    .get_matches();

    let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");

    let verbose = matches.is_present("verbose");
    let mut single = matches.is_present("single");
    let mut multi = matches.is_present("multi");
    let mut parallel = matches.is_present("parallel");

    if !single && !multi && !parallel {
        single = true;
        multi = true;
        parallel = true;
    }

    println!(
        "Running tests{}...",
        if verbose { " in verbose mode" } else { "" }
    );

    let mut comp_score: f64 = 0.0;

    if single {
        comp_score += single_doc_benchmarks(uri, verbose).unwrap();
    }
    if multi {
        comp_score += multi_doc_benchmarks(uri, verbose).unwrap();
    }
    if parallel {
        comp_score += parallel_benchmarks(uri, verbose).unwrap();
    }

    println!("----------------------------");
    println!("COMPOSITE SCORE = {}", comp_score);
}
