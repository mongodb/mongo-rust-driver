# Rust Driver Benchmark Suite

This suite implements the benchmarks described in this (spec)[https://github.com/mongodb/specifications/blob/master/source/benchmarking/benchmarking.md].

In order to run the microbenchmarks, first run `./download-data.sh`. (NOTE: the data for the deeply nested BSON encoding and decoding is
currently broken, so these benchmarks will not be runnable until that's fixed).

Note: make sure you run the download script and the microbenchmarks binary from the benchmark root (the directory containing this README).

To execute all benchmarks, run `cargo run --release` with a mongod instance running on port 27017 (or, you can specify a custom
connection string by setting the `MONGODB_URI` environment variable). You can specify a custom name for the used database or
collection by setting the `DATABASE_NAME` or `COLL_NAME` environment variables respectively.

Additionally, you can specify custom time frames for the benchmarks by setting the `MAX_EXECUTION_TIME`, `MIN_EXECUTION_TIME`
and `TARGET_ITERATION_COUNT` environment variables.

Run `cargo run --release -- --help` to see a full list of testing options.

When running the benchmarks with the `--ids` flag, you can refer to each benchmark by their unique id. You can refer to multiple 
benchmarks by separating each benchmark's id with a comma. For example `cargo run --release -- --ids 1,2,3,4` would run all 
the single-doc benchmarks. By default, all benchmarks are executed. The table below lists each benchmark's id.

| Benchmark                                      | ID |
|----------------------------------------------- |----|
| Run command                                    | 1  |
| Find one by ID                                 | 2  |
| Small doc insertOne                            | 3  |
| Large doc insertOne                            | 4  |
| Find many and empty the cursor                 | 5  |
| Small doc bulk insert                          | 6  |
| Large doc bulk insert                          | 7  |
| LDJSON multi-file import                       | 8  |
| LDJSON multi-file export                       | 9  |
| BSON flat decode                               | 10 |
| BSON flat encode                               | 11 |
| BSON deeply nested decode                      | 12 |
| BSON deeply nested encode                      | 13 |
| BSON full document decode                      | 14 |
| BSON full document encode                      | 15 |
| Find many and empty the cursor (raw BSON)      | 16 |
| Find many and empty the cursor (serde structs) | 17 |
| GridFS download                                | 18 |
| GridFS upload                                  | 19 |
| GridFS multi-file download                     | 20 |
| GridFS multi-file upload                       | 21 |
| All benchmarks                                 | all|

Note that in order to compare against the other drivers, an inMemory mongod instance should be used.

Also note that the parallel benchmarks are implemented to mirror the C++ driver's interpretation of the spec.
