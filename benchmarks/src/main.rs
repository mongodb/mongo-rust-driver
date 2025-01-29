macro_rules! spawn_blocking_and_await {
    ($blocking_call:expr) => {{
        tokio::task::spawn_blocking(move || $blocking_call)
            .await
            .unwrap()
    }};
}

fn spawn<T>(future: T) -> impl Future<Output = <T as Future>::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::task::spawn(future).map(|result| result.unwrap())
}

mod bench;
mod fs;
mod models;
mod score;

use std::{
    collections::HashSet,
    convert::TryFrom,
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::{App, Arg, ArgMatches};
use futures::{Future, FutureExt};
use mongodb::options::ClientOptions;
use once_cell::sync::Lazy;

use crate::{
    bench::{
        bson_decode::BsonDecodeBenchmark,
        bson_encode::BsonEncodeBenchmark,
        bulk_write::{InsertBulkWriteBenchmark, MixedBulkWriteBenchmark},
        find_many::FindManyBenchmark,
        find_one::FindOneBenchmark,
        gridfs_download::GridFsDownloadBenchmark,
        gridfs_multi_download::GridFsMultiDownloadBenchmark,
        gridfs_multi_upload::GridFsMultiUploadBenchmark,
        gridfs_upload::GridFsUploadBenchmark,
        insert_many::InsertManyBenchmark,
        insert_one::InsertOneBenchmark,
        json_multi_export::JsonMultiExportBenchmark,
        json_multi_import::JsonMultiImportBenchmark,
        run_command::RunCommandBenchmark,
    },
    fs::File,
    score::{score_test, BenchmarkResult, CompositeScore},
};

static DATA_PATH: Lazy<PathBuf> = Lazy::new(|| Path::new(env!("CARGO_MANIFEST_DIR")).join("data"));

// benchmark names
const FLAT_BSON_ENCODING: &'static str = "Flat BSON Encoding";
const FLAT_BSON_DECODING: &'static str = "Flat BSON Decoding";
const DEEP_BSON_ENCODING: &'static str = "Deep BSON Encoding";
const DEEP_BSON_DECODING: &'static str = "Deep BSON Decoding";
const FULL_BSON_ENCODING: &'static str = "Full BSON Encoding";
const FULL_BSON_DECODING: &'static str = "Full BSON Decoding";
const RUN_COMMAND_BENCH: &'static str = "Run Command";
const RUN_COMMAND_COLD_START_BENCH: &'static str = "Run Command (cold start)";
const FIND_ONE_BENCH: &'static str = "Find one";
const FIND_MANY_BENCH: &'static str = "Find many and empty cursor";
const FIND_MANY_BENCH_RAW: &'static str = "Find many and empty cursor (raw BSON)";
const FIND_MANY_BENCH_SERDE: &'static str = "Find many and empty cursor (serde structs)";
const GRIDFS_DOWNLOAD_BENCH: &'static str = "GridFS download";
const LDJSON_MULTI_EXPORT_BENCH: &'static str = "LDJSON multi-file export";
const GRIDFS_MULTI_DOWNLOAD_BENCH: &'static str = "GridFS multi-file download";
const SMALL_DOC_INSERT_ONE_BENCH: &'static str = "Small doc insertOne";
const LARGE_DOC_INSERT_ONE_BENCH: &'static str = "Large doc insertOne";
const SMALL_DOC_INSERT_MANY_BENCH: &'static str = "Small doc insertMany";
const LARGE_DOC_INSERT_MANY_BENCH: &'static str = "Large doc insertMany";
const GRIDFS_UPLOAD_BENCH: &'static str = "GridFS upload";
const LDJSON_MULTI_IMPORT_BENCH: &'static str = "LDJSON multi-file import";
const GRIDFS_MULTI_UPLOAD_BENCH: &'static str = "GridFS multi-file upload";
const SMALL_DOC_INSERT_BULK_WRITE_BENCH: &'static str = "Small doc insert-only bulkWrite";
const LARGE_DOC_INSERT_BULK_WRITE_BENCH: &'static str = "Large doc insert-only bulkWrite";
const MIXED_BULK_WRITE_BENCH: &'static str = "Mixed bulkWrite";

#[derive(Copy, Clone, num_enum::TryFromPrimitive, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[repr(u8)]
enum BenchmarkId {
    RunCommand = 1,
    FindOneById,             // 2
    SmallDocInsertOne,       // 3
    LargeDocInsertOne,       // 4
    FindMany,                // 5
    SmallDocInsertMany,      // 6
    LargeDocInsertMany,      // 7
    LdJsonMultiFileImport,   // 8
    LdJsonMultiFileExport,   // 9
    BsonFlatDocumentDecode,  // 10
    BsonFlatDocumentEncode,  // 11
    BsonDeepDocumentDecode,  // 12
    BsonDeepDocumentEncode,  // 13
    BsonFullDocumentDecode,  // 14
    BsonFullDocumentEncode,  // 15
    FindManyRawBson,         // 16
    FindManySerde,           // 17
    GridFsDownload,          // 18
    GridFsUpload,            // 19
    GridFsMultiDownload,     // 20
    GridFsMultiUpload,       // 21
    RunCommandColdStart,     // 22
    SmallDocInsertBulkWrite, // 23
    LargeDocInsertBulkWrite, // 24
    MixedBulkWrite,          // 25
}

impl BenchmarkId {
    fn name(self) -> &'static str {
        match self {
            BenchmarkId::RunCommand => RUN_COMMAND_BENCH,
            BenchmarkId::RunCommandColdStart => RUN_COMMAND_COLD_START_BENCH,
            BenchmarkId::FindOneById => FIND_ONE_BENCH,
            BenchmarkId::SmallDocInsertOne => SMALL_DOC_INSERT_ONE_BENCH,
            BenchmarkId::LargeDocInsertOne => LARGE_DOC_INSERT_ONE_BENCH,
            BenchmarkId::FindMany => FIND_MANY_BENCH,
            BenchmarkId::SmallDocInsertMany => SMALL_DOC_INSERT_MANY_BENCH,
            BenchmarkId::LargeDocInsertMany => LARGE_DOC_INSERT_MANY_BENCH,
            BenchmarkId::LdJsonMultiFileImport => LDJSON_MULTI_IMPORT_BENCH,
            BenchmarkId::LdJsonMultiFileExport => LDJSON_MULTI_EXPORT_BENCH,
            BenchmarkId::BsonFlatDocumentDecode => FLAT_BSON_DECODING,
            BenchmarkId::BsonFlatDocumentEncode => FLAT_BSON_ENCODING,
            BenchmarkId::BsonDeepDocumentDecode => DEEP_BSON_DECODING,
            BenchmarkId::BsonDeepDocumentEncode => DEEP_BSON_ENCODING,
            BenchmarkId::BsonFullDocumentDecode => FULL_BSON_DECODING,
            BenchmarkId::BsonFullDocumentEncode => FULL_BSON_ENCODING,
            BenchmarkId::FindManyRawBson => FIND_MANY_BENCH_RAW,
            BenchmarkId::FindManySerde => FIND_MANY_BENCH_SERDE,
            BenchmarkId::GridFsDownload => GRIDFS_DOWNLOAD_BENCH,
            BenchmarkId::GridFsUpload => GRIDFS_UPLOAD_BENCH,
            BenchmarkId::GridFsMultiDownload => GRIDFS_MULTI_DOWNLOAD_BENCH,
            BenchmarkId::GridFsMultiUpload => GRIDFS_MULTI_UPLOAD_BENCH,
            BenchmarkId::SmallDocInsertBulkWrite => SMALL_DOC_INSERT_BULK_WRITE_BENCH,
            BenchmarkId::LargeDocInsertBulkWrite => LARGE_DOC_INSERT_BULK_WRITE_BENCH,
            BenchmarkId::MixedBulkWrite => MIXED_BULK_WRITE_BENCH,
        }
    }
}

/// Benchmarks included in the "BSONBench" composite.
const BSON_BENCHES: &[&'static str] = &[
    FLAT_BSON_ENCODING,
    FLAT_BSON_DECODING,
    DEEP_BSON_ENCODING,
    DEEP_BSON_DECODING,
    FULL_BSON_ENCODING,
    FULL_BSON_DECODING,
];

/// Benchmarkes included in the "SingleBench" composite.
/// This consists of all the single-doc benchmarks except Run Command.
const SINGLE_BENCHES: &[&'static str] = &[
    FIND_ONE_BENCH,
    SMALL_DOC_INSERT_ONE_BENCH,
    LARGE_DOC_INSERT_ONE_BENCH,
];

/// Benchmarks included in the "MultiBench" composite.
const MULTI_BENCHES: &[&'static str] = &[
    FIND_MANY_BENCH_RAW,
    SMALL_DOC_INSERT_MANY_BENCH,
    LARGE_DOC_INSERT_MANY_BENCH,
    GRIDFS_UPLOAD_BENCH,
    GRIDFS_DOWNLOAD_BENCH,
    SMALL_DOC_INSERT_BULK_WRITE_BENCH,
    LARGE_DOC_INSERT_BULK_WRITE_BENCH,
    MIXED_BULK_WRITE_BENCH,
];

/// Benchmarks included in the "ParallelBench" composite.
const PARALLEL_BENCHES: &[&'static str] = &[
    LDJSON_MULTI_IMPORT_BENCH,
    LDJSON_MULTI_EXPORT_BENCH,
    GRIDFS_MULTI_UPLOAD_BENCH,
    GRIDFS_MULTI_DOWNLOAD_BENCH,
];

/// Benchmarks included in the "ReadBench" composite.
const READ_BENCHES: &[&'static str] = &[
    FIND_ONE_BENCH,
    FIND_MANY_BENCH_RAW,
    GRIDFS_DOWNLOAD_BENCH,
    LDJSON_MULTI_EXPORT_BENCH,
    GRIDFS_MULTI_DOWNLOAD_BENCH,
];

/// Benchmarks included in the "WriteBench" composite.
const WRITE_BENCHES: &[&'static str] = &[
    SMALL_DOC_INSERT_ONE_BENCH,
    LARGE_DOC_INSERT_ONE_BENCH,
    SMALL_DOC_INSERT_MANY_BENCH,
    LARGE_DOC_INSERT_MANY_BENCH,
    GRIDFS_UPLOAD_BENCH,
    LDJSON_MULTI_IMPORT_BENCH,
    GRIDFS_MULTI_UPLOAD_BENCH,
];

const MAX_ID: u8 = BenchmarkId::MixedBulkWrite as u8;

async fn run_benchmarks(
    uri: &str,
    more_info: bool,
    ids: HashSet<BenchmarkId>,
) -> Result<CompositeScore> {
    let options = ClientOptions::parse(uri).await?;

    let mut comp_score = CompositeScore::new("All Benchmarks");
    let mut sorted_ids: Vec<BenchmarkId> = ids.into_iter().collect();
    sorted_ids.sort();

    for id in sorted_ids {
        println!("Running {}...", id.name());

        match id {
            // Run command
            BenchmarkId::RunCommand => {
                let run_command_options = bench::run_command::Options {
                    num_iter: 10000,
                    uri: uri.to_string(),
                    cold_start: false,
                };
                let run_command =
                    bench::run_benchmark::<RunCommandBenchmark>(run_command_options).await?;

                comp_score += score_test(run_command, RUN_COMMAND_BENCH, 0.13, more_info);
            }

            // Run command, including client setup time
            BenchmarkId::RunCommandColdStart => {
                let run_command_options = bench::run_command::Options {
                    num_iter: 100,
                    uri: uri.to_string(),
                    cold_start: true,
                };
                let run_command =
                    bench::run_benchmark::<RunCommandBenchmark>(run_command_options).await?;

                comp_score +=
                    score_test(run_command, RUN_COMMAND_COLD_START_BENCH, 0.13, more_info);
            }

            // Small doc insertOne
            BenchmarkId::SmallDocInsertOne => {
                let small_insert_one_options = bench::insert_one::Options {
                    num_iter: 10000,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("small_doc.json"),
                    uri: uri.to_string(),
                };
                let small_insert_one =
                    bench::run_benchmark::<InsertOneBenchmark>(small_insert_one_options).await?;

                comp_score += score_test(
                    small_insert_one,
                    SMALL_DOC_INSERT_ONE_BENCH,
                    2.75,
                    more_info,
                );
            }

            // Large doc insertOne
            BenchmarkId::LargeDocInsertOne => {
                let large_insert_one_options = bench::insert_one::Options {
                    num_iter: 10,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("large_doc.json"),
                    uri: uri.to_string(),
                };
                let large_insert_one =
                    bench::run_benchmark::<InsertOneBenchmark>(large_insert_one_options).await?;

                comp_score += score_test(
                    large_insert_one,
                    LARGE_DOC_INSERT_ONE_BENCH,
                    27.31,
                    more_info,
                );
            }

            // Small doc bulk insert
            BenchmarkId::SmallDocInsertMany => {
                let small_insert_many_options = bench::insert_many::Options {
                    num_copies: 10000,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("small_doc.json"),
                    uri: uri.to_string(),
                };
                let small_insert_many =
                    bench::run_benchmark::<InsertManyBenchmark>(small_insert_many_options).await?;

                comp_score += score_test(
                    small_insert_many,
                    SMALL_DOC_INSERT_MANY_BENCH,
                    2.75,
                    more_info,
                );
            }

            // Large doc bulk insert
            BenchmarkId::LargeDocInsertMany => {
                let large_insert_many_options = bench::insert_many::Options {
                    num_copies: 10,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("large_doc.json"),
                    uri: uri.to_string(),
                };
                let large_insert_many =
                    bench::run_benchmark::<InsertManyBenchmark>(large_insert_many_options).await?;

                comp_score += score_test(
                    large_insert_many,
                    LARGE_DOC_INSERT_MANY_BENCH,
                    27.31,
                    more_info,
                );
            }

            // LDJSON multi-file import
            BenchmarkId::LdJsonMultiFileImport => {
                // can only run against standalones because otherwise the host machine will run out
                // of memory
                if options.hosts.len() > 1 {
                    continue;
                }
                let json_multi_import_options = bench::json_multi_import::Options {
                    path: DATA_PATH.join("parallel").join("ldjson_multi"),
                    uri: uri.to_string(),
                };
                let json_multi_import =
                    bench::run_benchmark::<JsonMultiImportBenchmark>(json_multi_import_options)
                        .await?;

                comp_score += score_test(
                    json_multi_import,
                    LDJSON_MULTI_IMPORT_BENCH,
                    565.0,
                    more_info,
                );
            }

            // LDJSON multi-file export
            BenchmarkId::LdJsonMultiFileExport => {
                // can only run against standalones because otherwise the host machine will run out
                // of memory
                if options.hosts.len() > 1 {
                    continue;
                }

                let json_multi_export_options = bench::json_multi_export::Options {
                    path: DATA_PATH.join("parallel").join("ldjson_multi"),
                    uri: uri.to_string(),
                };
                let json_multi_export =
                    bench::run_benchmark::<JsonMultiExportBenchmark>(json_multi_export_options)
                        .await?;

                comp_score += score_test(
                    json_multi_export,
                    LDJSON_MULTI_EXPORT_BENCH,
                    565.0,
                    more_info,
                );
            }

            // BSON flat document decode
            BenchmarkId::BsonFlatDocumentDecode => {
                let bson_flat_decode_options = bench::bson_decode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("flat_bson.json"),
                };
                let bson_flat_decode =
                    bench::run_benchmark::<BsonDecodeBenchmark>(bson_flat_decode_options).await?;

                comp_score += score_test(bson_flat_decode, FLAT_BSON_DECODING, 75.31, more_info);
            }

            // BSON flat document encode
            BenchmarkId::BsonFlatDocumentEncode => {
                let bson_flat_encode_options = bench::bson_encode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("flat_bson.json"),
                };
                let bson_flat_encode =
                    bench::run_benchmark::<BsonEncodeBenchmark>(bson_flat_encode_options).await?;

                comp_score += score_test(bson_flat_encode, FLAT_BSON_ENCODING, 75.31, more_info);
            }

            // BSON deep document decode
            BenchmarkId::BsonDeepDocumentDecode => {
                let bson_deep_decode_options = bench::bson_decode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("deep_bson.json"),
                };
                let bson_deep_decode =
                    bench::run_benchmark::<BsonDecodeBenchmark>(bson_deep_decode_options).await?;

                comp_score += score_test(bson_deep_decode, DEEP_BSON_DECODING, 19.64, more_info);
            }

            // BSON deep document encode
            BenchmarkId::BsonDeepDocumentEncode => {
                let bson_deep_encode_options = bench::bson_encode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("deep_bson.json"),
                };
                let bson_deep_encode =
                    bench::run_benchmark::<BsonEncodeBenchmark>(bson_deep_encode_options).await?;

                comp_score += score_test(bson_deep_encode, DEEP_BSON_ENCODING, 19.64, more_info);
            }

            // BSON full document decode
            BenchmarkId::BsonFullDocumentDecode => {
                let bson_full_decode_options = bench::bson_decode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("full_bson.json"),
                };
                let bson_full_decode =
                    bench::run_benchmark::<BsonDecodeBenchmark>(bson_full_decode_options).await?;

                comp_score += score_test(bson_full_decode, FULL_BSON_DECODING, 57.34, more_info);
            }

            // BSON full document encode
            BenchmarkId::BsonFullDocumentEncode => {
                let bson_full_encode_options = bench::bson_encode::Options {
                    num_iter: 10_000,
                    path: DATA_PATH.join("extended_bson").join("full_bson.json"),
                };
                let bson_full_encode =
                    bench::run_benchmark::<BsonEncodeBenchmark>(bson_full_encode_options).await?;

                comp_score += score_test(bson_full_encode, FULL_BSON_ENCODING, 57.34, more_info);
            }

            // Find one by ID
            BenchmarkId::FindOneById => {
                let find_one_options = bench::find_one::Options {
                    num_iter: 10000,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("tweet.json"),
                    uri: uri.to_string(),
                };
                let find_one = bench::run_benchmark::<FindOneBenchmark>(find_one_options).await?;

                comp_score += score_test(find_one, id.name(), 16.22, more_info);
            }

            // Find many and empty the cursor
            BenchmarkId::FindManyRawBson | BenchmarkId::FindMany | BenchmarkId::FindManySerde => {
                let mode = match id {
                    BenchmarkId::FindMany => bench::find_many::Mode::Document,
                    BenchmarkId::FindManyRawBson => bench::find_many::Mode::RawBson,
                    BenchmarkId::FindManySerde => bench::find_many::Mode::Serde,
                    _ => unreachable!(),
                };
                let find_many_options = bench::find_many::Options {
                    num_iter: 10000,
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("tweet.json"),
                    uri: uri.to_string(),
                    mode,
                };
                let find_many =
                    bench::run_benchmark::<FindManyBenchmark>(find_many_options).await?;

                comp_score += score_test(find_many, id.name(), 16.22, more_info);
            }

            // GridFS download
            BenchmarkId::GridFsDownload => {
                let gridfs_download_options = bench::gridfs_download::Options {
                    uri: uri.to_string(),
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("gridfs_large.bin"),
                };
                let gridfs_download =
                    bench::run_benchmark::<GridFsDownloadBenchmark>(gridfs_download_options)
                        .await?;

                comp_score += score_test(gridfs_download, id.name(), 52.43, more_info);
            }

            // GridFS upload
            BenchmarkId::GridFsUpload => {
                let gridfs_upload_options = bench::gridfs_upload::Options {
                    uri: uri.to_string(),
                    path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("gridfs_large.bin"),
                };
                let gridfs_upload =
                    bench::run_benchmark::<GridFsUploadBenchmark>(gridfs_upload_options).await?;

                comp_score += score_test(gridfs_upload, id.name(), 52.43, more_info);
            }

            // GridFS multi download
            BenchmarkId::GridFsMultiDownload => {
                let gridfs_multi_download_options = bench::gridfs_multi_download::Options {
                    uri: uri.to_string(),
                    path: DATA_PATH.join("parallel").join("gridfs_multi"),
                };
                let gridfs_multi_download = bench::run_benchmark::<GridFsMultiDownloadBenchmark>(
                    gridfs_multi_download_options,
                )
                .await?;

                comp_score += score_test(gridfs_multi_download, id.name(), 262.144, more_info);
            }

            // GridFS multi upload
            BenchmarkId::GridFsMultiUpload => {
                let gridfs_multi_upload_options = bench::gridfs_multi_upload::Options {
                    uri: uri.to_string(),
                    path: DATA_PATH.join("parallel").join("gridfs_multi"),
                };
                let gridfs_multi_upload =
                    bench::run_benchmark::<GridFsMultiUploadBenchmark>(gridfs_multi_upload_options)
                        .await?;

                comp_score += score_test(gridfs_multi_upload, id.name(), 262.144, more_info);
            }

            // Small doc insert-only bulk write
            BenchmarkId::SmallDocInsertBulkWrite => {
                let bulk_write_options = bench::bulk_write::Options {
                    uri: uri.to_string(),
                    data_path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("small_doc.json"),
                    num_models: 10_000,
                };
                let small_doc_insert_bulk_write =
                    bench::run_benchmark::<InsertBulkWriteBenchmark>(bulk_write_options).await?;
                comp_score += score_test(small_doc_insert_bulk_write, id.name(), 2.75, more_info);
            }

            // Large doc insert-only bulk write
            BenchmarkId::LargeDocInsertBulkWrite => {
                let bulk_write_options = bench::bulk_write::Options {
                    uri: uri.to_string(),
                    data_path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("large_doc.json"),
                    num_models: 10,
                };
                let large_doc_insert_bulk_write =
                    bench::run_benchmark::<InsertBulkWriteBenchmark>(bulk_write_options).await?;
                comp_score += score_test(large_doc_insert_bulk_write, id.name(), 27.31, more_info);
            }

            // Mixed bulk write
            BenchmarkId::MixedBulkWrite => {
                let bulk_write_options = bench::bulk_write::Options {
                    uri: uri.to_string(),
                    data_path: DATA_PATH
                        .join("single_and_multi_document")
                        .join("small_doc.json"),
                    num_models: 30_000,
                };
                let mixed_bulk_write =
                    bench::run_benchmark::<MixedBulkWriteBenchmark>(bulk_write_options).await?;
                comp_score += score_test(mixed_bulk_write, id.name(), 5.5, more_info);
            }
        }
    }

    Ok(comp_score)
}

fn parse_ids(matches: ArgMatches) -> HashSet<BenchmarkId> {
    let mut ids: HashSet<BenchmarkId> = match matches.value_of("ids") {
        Some("all") => (1..=MAX_ID)
            .map(|id| BenchmarkId::try_from(id as u8).unwrap())
            .collect(),
        Some(id_list) => id_list
            .split(',')
            .map(|str| {
                let n = str
                    .parse::<u8>()
                    .expect("invalid test IDs provided, see README");
                BenchmarkId::try_from(n).expect("invalid test IDs provided, see README")
            })
            .collect(),
        None => HashSet::new(),
    };

    if matches.is_present("single") {
        ids.insert(BenchmarkId::RunCommand);
        ids.insert(BenchmarkId::RunCommandColdStart);
        ids.insert(BenchmarkId::FindOneById);
        ids.insert(BenchmarkId::SmallDocInsertOne);
        ids.insert(BenchmarkId::LargeDocInsertOne);
    }
    if matches.is_present("multi") {
        ids.insert(BenchmarkId::FindManyRawBson);
        ids.insert(BenchmarkId::SmallDocInsertMany);
        ids.insert(BenchmarkId::LargeDocInsertMany);
        ids.insert(BenchmarkId::GridFsDownload);
        ids.insert(BenchmarkId::GridFsUpload);
        ids.insert(BenchmarkId::SmallDocInsertBulkWrite);
        ids.insert(BenchmarkId::LargeDocInsertBulkWrite);
        ids.insert(BenchmarkId::MixedBulkWrite);
    }
    if matches.is_present("parallel") {
        ids.insert(BenchmarkId::LdJsonMultiFileImport);
        ids.insert(BenchmarkId::LdJsonMultiFileExport);
        ids.insert(BenchmarkId::GridFsMultiDownload);
        // TODO RUST-2010 Re-enable this benchmark
        //ids.insert(BenchmarkId::GridFsMultiUpload);
    }
    if matches.is_present("bson") {
        ids.insert(BenchmarkId::BsonFlatDocumentDecode);
        ids.insert(BenchmarkId::BsonFlatDocumentEncode);
        ids.insert(BenchmarkId::BsonDeepDocumentDecode);
        ids.insert(BenchmarkId::BsonDeepDocumentEncode);
        ids.insert(BenchmarkId::BsonFullDocumentDecode);
        ids.insert(BenchmarkId::BsonFullDocumentEncode);
    }
    if matches.is_present("driver") {
        ids.insert(BenchmarkId::RunCommand);
        ids.insert(BenchmarkId::RunCommandColdStart);
        ids.insert(BenchmarkId::FindOneById);
        ids.insert(BenchmarkId::SmallDocInsertOne);
        ids.insert(BenchmarkId::LargeDocInsertOne);
        ids.insert(BenchmarkId::FindMany);
        ids.insert(BenchmarkId::FindManyRawBson);
        ids.insert(BenchmarkId::FindManySerde);
        ids.insert(BenchmarkId::SmallDocInsertMany);
        ids.insert(BenchmarkId::LargeDocInsertMany);
        ids.insert(BenchmarkId::LdJsonMultiFileImport);
        ids.insert(BenchmarkId::LdJsonMultiFileExport);
        ids.insert(BenchmarkId::GridFsDownload);
        ids.insert(BenchmarkId::GridFsUpload);
        ids.insert(BenchmarkId::GridFsMultiDownload);
        // TODO RUST-2010 Re-enable this benchmark
        //ids.insert(BenchmarkId::GridFsMultiUpload);
    }

    // if none were enabled, that means no arguments were provided and all should be enabled.
    if ids.is_empty() {
        ids = (1..=MAX_ID)
            .map(|id| BenchmarkId::try_from(id as u8).unwrap())
            // TODO RUST-2010 Re-enable this benchmark
            .filter(|id| *id != BenchmarkId::GridFsMultiUpload)
            .collect()
    }

    ids
}

#[tokio::main]
async fn main() {
    // ensure MAX_ID is kept up to date.
    assert!(
        BenchmarkId::try_from(MAX_ID + 1).is_err(),
        "MAX_ID not up to date"
    );

    let mut id_help = String::from("\nRun benchmarks by id number (comma-separated):\n");
    for ix in 1..=MAX_ID {
        let id = BenchmarkId::try_from(ix).unwrap();
        id_help.push_str(&format!("    {}: {}\n", ix, id.name()));
    }
    id_help.push_str("    all: All benchmarks\n                    ");

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
            Arg::with_name("driver")
                .short("d")
                .long("driver")
                .help("Run driver-only benchmarks"),
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
                .long_help(&id_help),
        )
        .arg(
            Arg::with_name("output")
                .short("o")
                .long("output")
                .takes_value(true)
                .help("Output file to contain the JSON data to be ingested by Evergreen"),
        )
        .get_matches();

    let uri = option_env!("MONGODB_URI").unwrap_or("mongodb://localhost:27017");

    let verbose = matches.is_present("verbose");
    let output_file = matches.value_of("output").map(|p| PathBuf::new().join(p));

    println!(
        "Running tests{}...\n",
        if verbose {
            " in verbose mode"
        } else {
            " nonverbosely"
        }
    );

    let ids = parse_ids(matches);
    let scores = run_benchmarks(uri, verbose, ids).await.unwrap();

    let read_bench = scores.filter("ReadBench", READ_BENCHES);
    let write_bench = scores.filter("WriteBench", WRITE_BENCHES);
    let mut driver_bench = CompositeScore::new("DriverBench");
    driver_bench += read_bench.clone();
    driver_bench += write_bench.clone();

    let composite_scores: Vec<CompositeScore> = vec![
        scores.filter("BSONBench", BSON_BENCHES),
        scores.filter("SingleBench", SINGLE_BENCHES),
        scores.filter("MultiBench", MULTI_BENCHES),
        scores.filter("ParallelBench", PARALLEL_BENCHES),
        read_bench,
        write_bench,
        driver_bench,
    ]
    .into_iter()
    .filter(|s| s.count() > 0)
    .collect();

    for score in composite_scores.iter() {
        println!("{}", score);
    }

    if let Some(output_file) = output_file {
        // attach the individual benchmark results
        let mut results: Vec<BenchmarkResult> = scores.to_individual_results();

        // then the composite ones
        results.extend(
            composite_scores
                .into_iter()
                .map(CompositeScore::into_single_result),
        );

        let mut file = File::open_write(&output_file).await.unwrap();
        file.write_line(serde_json::to_string_pretty(&results).unwrap().as_str())
            .await
            .unwrap();
        file.flush().await.unwrap();
    }
}
