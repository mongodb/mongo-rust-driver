#[macro_use]
macro_rules! spawn_blocking_and_await {
    ($blocking_call:expr) => {{
        #[cfg(feature = "tokio-runtime")]
        {
            tokio::task::spawn_blocking(move || $blocking_call)
                .await
                .unwrap()
        }

        #[cfg(feature = "async-std-runtime")]
        {
            async_std::task::spawn_blocking(move || $blocking_call).await
        }
    }};
}

mod bench;
mod fs;

use std::{
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::Result;
use clap::{App, Arg, ArgMatches};
use lazy_static::lazy_static;

use crate::bench::{
    bson_decode::BsonDecodeBenchmark,
    bson_encode::BsonEncodeBenchmark,
    find_many::FindManyBenchmark,
    find_one::FindOneBenchmark,
    insert_many::InsertManyBenchmark,
    insert_one::InsertOneBenchmark,
    json_multi_export::JsonMultiExportBenchmark,
    json_multi_import::JsonMultiImportBenchmark,
    run_command::RunCommandBenchmark,
    TARGET_ITERATION_COUNT,
};

lazy_static! {
    static ref DATA_PATH: PathBuf = Path::new(env!("CARGO_MANIFEST_DIR")).join("data");
}

fn get_nth_percentile(durations: &[Duration], n: f64) -> Duration {
    let index = (durations.len() as f64 * (n / 100.0)) as usize;
    durations[std::cmp::max(index, 1) - 1]
}

fn score_test(durations: Vec<Duration>, name: &str, task_size: f64, more_info: bool) -> f64 {
    let median = get_nth_percentile(&durations, 50.0);
    let score = task_size / (median.as_millis() as f64 / 1000.0);
    println!("TEST: {} -- Score: {}\n", name, score);

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
            "99th percentile: {:#?}\n",
            get_nth_percentile(&durations, 99.0),
        );
    }

    score
}

async fn single_doc_benchmarks(uri: &str, more_info: bool, ids: &[bool]) -> Result<f64> {
    println!("----------------------------");
    println!("Single-Doc Benchmarks:");
    println!("----------------------------\n");

    let mut comp_score: f64 = 0.0;
    let mut benchmark_count = 0_usize;

    // Run command
    if ids[0] {
        let run_command_options = bench::run_command::Options {
            num_iter: 10000,
            uri: uri.to_string(),
        };
        println!("Running Run command...");
        let run_command = bench::run_benchmark::<RunCommandBenchmark>(run_command_options).await?;

        comp_score += score_test(run_command, "Run command", 0.16, more_info);
        benchmark_count += 1;
    }

    // Find one by ID
    if ids[1] {
        let find_one_options = bench::find_one::Options {
            num_iter: 10000,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("tweet.json"),
            uri: uri.to_string(),
        };
        println!("Running Find one by ID...");
        let find_one = bench::run_benchmark::<FindOneBenchmark>(find_one_options).await?;

        comp_score += score_test(find_one, "Find one by ID", 16.22, more_info);
        benchmark_count += 1;
    }

    // Small doc insertOne
    if ids[2] {
        let small_insert_one_options = bench::insert_one::Options {
            num_iter: 10000,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("small_doc.json"),
            uri: uri.to_string(),
        };
        println!("Running Small doc insertOne...");
        let small_insert_one =
            bench::run_benchmark::<InsertOneBenchmark>(small_insert_one_options).await?;

        comp_score += score_test(small_insert_one, "Small doc insertOne", 2.75, more_info);
        benchmark_count += 1;
    }

    // Large doc insertOne
    if ids[3] {
        let large_insert_one_options = bench::insert_one::Options {
            num_iter: 10,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("large_doc.json"),
            uri: uri.to_string(),
        };
        println!("Running Large doc insertOne...");
        let large_insert_one =
            bench::run_benchmark::<InsertOneBenchmark>(large_insert_one_options).await?;

        comp_score += score_test(large_insert_one, "Large doc insertOne", 27.31, more_info);
        benchmark_count += 1;
    }

    // Take average of total.
    comp_score /= benchmark_count as f64;

    println!("\nSingle-doc benchmark composite score: {}\n", comp_score);
    Ok(comp_score)
}

async fn multi_doc_benchmarks(uri: &str, more_info: bool, ids: &[bool]) -> Result<f64> {
    println!("----------------------------");
    println!("Multi-Doc Benchmarks:");
    println!("----------------------------\n");

    let mut comp_score: f64 = 0.0;
    let mut benchmark_count = 0_usize;

    // Find many and empty the cursor
    if ids[4] {
        let find_many_options = bench::find_many::Options {
            num_iter: 10000,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("tweet.json"),
            uri: uri.to_string(),
        };
        println!("Running Find many and empty the cursor...");
        let find_many = bench::run_benchmark::<FindManyBenchmark>(find_many_options).await?;

        comp_score += score_test(
            find_many,
            "Find many and empty the cursor",
            16.22,
            more_info,
        );
        benchmark_count += 1;
    }

    // Small doc bulk insert
    if ids[5] {
        let small_insert_many_options = bench::insert_many::Options {
            num_copies: 10000,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("small_doc.json"),
            uri: uri.to_string(),
        };
        println!("Running Small doc bulk insert...");
        let small_insert_many =
            bench::run_benchmark::<InsertManyBenchmark>(small_insert_many_options).await?;

        comp_score += score_test(small_insert_many, "Small doc bulk insert", 2.75, more_info);
        benchmark_count += 1;
    }

    // Large doc bulk insert
    if ids[6] {
        let large_insert_many_options = bench::insert_many::Options {
            num_copies: 10,
            path: DATA_PATH
                .join("single_and_multi_document")
                .join("large_doc.json"),
            uri: uri.to_string(),
        };
        println!("Running Large doc bulk insert...");
        let large_insert_many =
            bench::run_benchmark::<InsertManyBenchmark>(large_insert_many_options).await?;

        comp_score += score_test(large_insert_many, "Large doc bulk insert", 27.31, more_info);
        benchmark_count += 1;
    }

    // Take average of total.
    comp_score /= benchmark_count as f64;

    println!("\nMulti-doc benchmark composite score: {}\n", comp_score);
    Ok(comp_score)
}

async fn parallel_benchmarks(uri: &str, more_info: bool, ids: &[bool]) -> Result<f64> {
    println!("----------------------------");
    println!("Parallel Benchmarks:");
    println!("----------------------------\n");

    let mut comp_score: f64 = 0.0;
    let mut benchmark_count = 0_usize;

    // LDJSON multi-file import
    if ids[7] {
        let json_multi_import_options = bench::json_multi_import::Options {
            path: DATA_PATH.join("parallel").join("ldjson_multi"),
            uri: uri.to_string(),
        };
        println!("Running LDJSON multi-file import...");
        let json_multi_import =
            bench::run_benchmark::<JsonMultiImportBenchmark>(json_multi_import_options).await?;

        comp_score += score_test(
            json_multi_import,
            "LDJSON multi-file import",
            565.0,
            more_info,
        );
        benchmark_count += 1;
    }

    // LDJSON multi-file export
    if ids[8] {
        let json_multi_export_options = bench::json_multi_export::Options {
            path: DATA_PATH.join("parallel").join("ldjson_multi"),
            uri: uri.to_string(),
        };
        println!("Running LDJSON multi-file export...");
        let json_multi_export =
            bench::run_benchmark::<JsonMultiExportBenchmark>(json_multi_export_options).await?;

        comp_score += score_test(
            json_multi_export,
            "LDJSON multi-file export",
            565.0,
            more_info,
        );
        benchmark_count += 1;
    }

    // Take average of total.
    comp_score /= benchmark_count as f64;

    println!("\nParallel benchmark composite score: {}\n", comp_score);
    Ok(comp_score)
}

async fn bson_benchmarks(more_info: bool, ids: &[bool]) -> Result<f64> {
    println!("----------------------------");
    println!("BSON Benchmarks:");
    println!("----------------------------\n");

    let mut comp_score: f64 = 0.0;
    let mut benchmark_count = 0;

    // BSON flat document decode
    if ids[9] {
        let bson_flat_decode_options = bench::bson_decode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("flat_bson.json"),
        };
        println!("Running BSON flat decode...");
        let bson_flat_decode =
            bench::run_benchmark::<BsonDecodeBenchmark>(bson_flat_decode_options).await?;

        comp_score += score_test(bson_flat_decode, "BSON flat decode", 75.31, more_info);
        benchmark_count += 1;
    }

    // BSON flat document encode
    if ids[10] {
        let bson_flat_encode_options = bench::bson_encode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("flat_bson.json"),
        };
        println!("Running BSON flat encode...");
        let bson_flat_encode =
            bench::run_benchmark::<BsonEncodeBenchmark>(bson_flat_encode_options).await?;

        comp_score += score_test(bson_flat_encode, "BSON flat encode", 75.31, more_info);
        benchmark_count += 1;
    }

    // BSON deep document decode
    if ids[11] {
        let bson_deep_decode_options = bench::bson_decode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("deep_bson.json"),
        };
        println!("Running BSON deep decode...");
        let bson_deep_decode =
            bench::run_benchmark::<BsonDecodeBenchmark>(bson_deep_decode_options).await?;

        comp_score += score_test(bson_deep_decode, "BSON deep decode", 19.64, more_info);
        benchmark_count += 1;
    }

    // BSON deep document encode
    if ids[12] {
        let bson_deep_encode_options = bench::bson_encode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("deep_bson.json"),
        };
        println!("Running BSON deep encode...");
        let bson_deep_encode =
            bench::run_benchmark::<BsonEncodeBenchmark>(bson_deep_encode_options).await?;

        comp_score += score_test(bson_deep_encode, "BSON deep encode", 19.64, more_info);
        benchmark_count += 1;
    }

    // BSON full document decode
    if ids[13] {
        let bson_full_decode_options = bench::bson_decode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("full_bson.json"),
        };
        println!("Running BSON full decode...");
        let bson_full_decode =
            bench::run_benchmark::<BsonDecodeBenchmark>(bson_full_decode_options).await?;

        comp_score += score_test(bson_full_decode, "BSON full decode", 57.34, more_info);
        benchmark_count += 1;
    }

    // BSON full document encode
    if ids[14] {
        let bson_full_encode_options = bench::bson_encode::Options {
            num_iter: 10_000,
            path: DATA_PATH.join("extended_bson").join("full_bson.json"),
        };
        println!("Running BSON full encode...");
        let bson_full_encode =
            bench::run_benchmark::<BsonEncodeBenchmark>(bson_full_encode_options).await?;

        comp_score += score_test(bson_full_encode, "BSON full encode", 57.34, more_info);
        benchmark_count += 1;
    }

    // Take average of total.
    comp_score /= benchmark_count as f64;

    println!("\nBSON benchmark composite score: {}\n", comp_score);
    Ok(comp_score)
}

fn parse_ids(matches: ArgMatches) -> Vec<bool> {
    let id_list: Vec<usize> = match matches.value_of("ids") {
        Some("all") | None => vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
        Some(id_list) => id_list
            .split(',')
            .map(|str| {
                str.parse::<usize>()
                    .expect("invalid test IDs provided, see README")
            })
            .collect(),
    };

    let mut ids = vec![false; 15];
    for id in id_list {
        if id < 1 || id > 15 {
            panic!("invalid test IDs provided, see README");
        }
        ids[id - 1] = true;
    }

    if matches.is_present("single") {
        ids[0] = true;
        ids[1] = true;
        ids[2] = true;
        ids[3] = true;
    }
    if matches.is_present("multi") {
        ids[4] = true;
        ids[5] = true;
        ids[6] = true;
    }
    if matches.is_present("parallel") {
        ids[7] = true;
        ids[8] = true;
    }
    if matches.is_present("bson") {
        ids[9] = true;
        ids[10] = true;
        ids[11] = true;
        ids[12] = true;
        ids[13] = true;
        ids[14] = true;
        ids[15] = true;
    }

    ids
}

#[cfg_attr(feature = "tokio-runtime", tokio::main)]
#[cfg_attr(feature = "async-std-runtime", async_std::main)]
async fn main() {
    let matches = App::new("RustDriverBenchmark")
        .version(env!("CARGO_PKG_VERSION"))
        .about("Runs performance micro-bench")
        .author("benjirewis")
        .arg(
            Arg::with_name("single")
                .short("s")
                .long("single")
                .help("Run single document benchmarks"),
        )
        .arg(
            Arg::with_name("multi")
                .short("m")
                .long("multi")
                .help("Run multi document benchmarks"),
        )
        .arg(
            Arg::with_name("parallel")
                .short("p")
                .long("parallel")
                .help("Run parallel document benchmarks"),
        )
        .arg(
            Arg::with_name("bson")
                .short("b")
                .long("bson")
                .help("Run BSON-only benchmarks"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Print test information verbosely"),
        )
        .arg(
            Arg::with_name("ids")
                .short("i")
                .long("ids")
                .takes_value(true)
                .help("Run benchmarks by id number (comma-separated)")
                .long_help(
                    "
Run benchmarks by id number (comma-separated):
    1: Run command
    2: Find one by ID
    3: Small doc insertOne
    4: Large doc insertOne
    5: Find many and empty the cursor
    6: Small doc bulk insert
    7: Large doc bulk insert
    8: LDJSON multi-file import
    9: LDJSON multi-file export
    10: BSON flat document decode
    11: BSON flat document encode
    12: BSON deeply nested document decode
    13: BSON deeply nested document encode
    14: BSON full document decode
    15: BSON full document encode
    all: All benchmarks
                    ",
                ),
        )
        .get_matches();

    let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");

    let verbose = matches.is_present("verbose");

    println!(
        "Running tests{}...\n",
        if verbose {
            " in verbose mode"
        } else {
            " nonverbosely"
        }
    );

    let ids = parse_ids(matches);

    let mut comp_score: f64 = 0.0;

    // Single
    if ids[0] || ids[1] || ids[2] || ids[3] {
        comp_score += single_doc_benchmarks(uri, verbose, &ids).await.unwrap();
    }
    // Multi
    if ids[4] || ids[5] || ids[6] {
        comp_score += multi_doc_benchmarks(uri, verbose, &ids).await.unwrap();
    }
    // Parallel
    if ids[7] || ids[8] {
        comp_score += parallel_benchmarks(uri, verbose, &ids).await.unwrap();
    }

    // Bson
    if ids[9] || ids[10] || ids[11] || ids[12] || ids[13] || ids[14] {
        // BSON benchmarks are not computed as part of the composite score for the driver since
        // encoding/decoding is already part of all the other tasks.
        bson_benchmarks(verbose, &ids).await.unwrap();
    }

    println!("----------------------------");
    println!("Driver benchmark composite score = {}", comp_score);
}
