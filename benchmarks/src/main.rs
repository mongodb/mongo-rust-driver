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

use crate::{
    bench::{
        find_many::FindManyBenchmark, find_one::FindOneBenchmark, insert_many::InsertManyBenchmark,
        insert_one::InsertOneBenchmark, json_multi_export::JsonMultiExportBenchmark,
        json_multi_import::JsonMultiImportBenchmark, run_command::RunCommandBenchmark,
    },
    error::Result,
};

lazy_static! {
    static ref DATA_PATH: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("data");
}

fn get_nth_percentile(durations: &[Duration], n: f64) -> Duration {
    durations[(durations.len() as f64 * n / 100.0) as usize - 1]
}

fn score_test(durations: Vec<Duration>, name: &str, task_size: f64, more_info: bool) -> f64 {
    let score: f64 = get_nth_percentile(&durations, 50.0).as_millis() as f64 / task_size;

    println!("TEST: {} -- Score: {}", name, score);
    if more_info {
        println!(
            "10th percentile: {:#?}",
            get_nth_percentile(&durations, 10.0),
        );
        println!(
            "25th percentile: {:#?}",
            get_nth_percentile(&durations, 25.0),
        );
        println!(
            "50th percentile: {:#?}",
            get_nth_percentile(&durations, 50.0),
        );
        println!(
            "75th percentile: {:#?}",
            get_nth_percentile(&durations, 75.0),
        );
        println!(
            "90th percentile: {:#?}",
            get_nth_percentile(&durations, 90.0),
        );
        println!(
            "95th percentile: {:#?}",
            get_nth_percentile(&durations, 95.0),
        );
        println!(
            "98th percentile: {:#?}",
            get_nth_percentile(&durations, 98.0),
        );
        println!(
            "99th percentile: {:#?}",
            get_nth_percentile(&durations, 99.0),
        );
    }

    score
}

fn single_doc_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("----------------------------");
    println!("Single-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Run command
    let run_command_options = bench::run_command::Options {
        num_iter: 10000,
        uri: uri.to_string(),
    };
    let run_command = bench::run_benchmark::<RunCommandBenchmark>(run_command_options)?;

    comp_score += score_test(run_command, "Run command", 0.16, more_info);

    // Find one by ID
    let find_one_options = bench::find_one::Options {
        num_iter: 10000,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("tweet.json"),
        uri: uri.to_string(),
    };
    let find_one = bench::run_benchmark::<FindOneBenchmark>(find_one_options)?;

    comp_score += score_test(find_one, "Find one by ID", 16.22, more_info);

    // Small doc insertOne
    let small_insert_one_options = bench::insert_one::Options {
        num_iter: 10000,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("small_doc.json"),
        uri: uri.to_string(),
    };
    let small_insert_one = bench::run_benchmark::<InsertOneBenchmark>(small_insert_one_options)?;

    comp_score += score_test(small_insert_one, "Small doc insertOne", 2.75, more_info);

    // Large doc insertOne
    let large_insert_one_options = bench::insert_one::Options {
        num_iter: 10,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("large_doc.json"),
        uri: uri.to_string(),
    };
    let large_insert_one = bench::run_benchmark::<InsertOneBenchmark>(large_insert_one_options)?;

    comp_score += score_test(large_insert_one, "Large doc insertOne", 27.31, more_info);

    Ok(comp_score)
}

fn multi_doc_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("----------------------------");
    println!("Multi-Doc Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // Find many and empty the cursor
    let find_many_options = bench::find_many::Options {
        num_iter: 10000,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("tweet.json"),
        uri: uri.to_string(),
    };
    let find_many = bench::run_benchmark::<FindManyBenchmark>(find_many_options)?;

    comp_score += score_test(
        find_many,
        "Find many and empty the cursor",
        16.22,
        more_info,
    );

    // Small doc bulk insert
    let small_insert_many_options = bench::insert_many::Options {
        num_copies: 10000,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("small_doc.json"),
        uri: uri.to_string(),
    };
    let small_insert_many = bench::run_benchmark::<InsertManyBenchmark>(small_insert_many_options)?;

    comp_score += score_test(small_insert_many, "Small doc bulk insert", 2.75, more_info);

    // Large doc bulk insert
    let large_insert_many_options = bench::insert_many::Options {
        num_copies: 10,
        path: DATA_PATH
            .join("single_and_multi_document")
            .join("large_doc.json"),
        uri: uri.to_string(),
    };
    let large_insert_many = bench::run_benchmark::<InsertManyBenchmark>(large_insert_many_options)?;

    comp_score += score_test(large_insert_many, "Large doc bulk insert", 27.31, more_info);

    Ok(comp_score)
}

fn parallel_benchmarks(uri: &str, more_info: bool) -> Result<f64> {
    println!("----------------------------");
    println!("Parallel Benchmarks:");
    println!("----------------------------");

    let mut comp_score: f64 = 0.0;

    // LDJSON multi-file import
    let json_multi_import_options = bench::json_multi_import::Options {
        num_threads: num_cpus::get(),
        path: DATA_PATH.join("parallel").join("ldjson_multi"),
        uri: uri.to_string(),
    };
    let json_multi_import =
        bench::run_benchmark::<JsonMultiImportBenchmark>(json_multi_import_options)?;

    comp_score += score_test(
        json_multi_import,
        "LDJSON multi-file import",
        565.0,
        more_info,
    );

    // LDJSON multi-file export
    let json_multi_export_options = bench::json_multi_export::Options {
        num_threads: num_cpus::get(),
        path: DATA_PATH.join("parallel").join("ldjson_multi"),
        uri: uri.to_string(),
    };
    let json_multi_export =
        bench::run_benchmark::<JsonMultiExportBenchmark>(json_multi_export_options)?;

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
        (version: env!("CARGO_PKG_VERSION"))
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
