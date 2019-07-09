# Rust Driver Benchmark Suite

This suite implements the benchmarks described in this (spec)[https://github.com/mongodb/specifications/blob/master/source/benchmarking/benchmarking.rst].

In order to run the microbenchmarks, first run `../etc/microbenchmark-test-data.sh` to download the data.

Note: make sure you run the download script and the microbenchmarks binary from the benchmark root (the directory containing this README).

To execute all benchmarks, run `cargo run --release` with a mongod instance running on port 27017 (or, you can specify a custom
connection string by setting the `MONGODB_URI` environment variable). You can specify a custom name for the used database or
collection by setting the `DATABASE_NAME` or `COLL_NAME` environment variables respectively.

Additionally, you can specify custom time frames for the benchmarks by setting the `MAX_EXECUTION_TIME`, `MIN_EXECUTION_TIME`
and `MAX_ITERATIONS` environment variables.

Run `cargo run --release -- --help` to see a full list of testing options.

Note that in order to compare against the other drivers, an inMemory mongod instance should be used.

At this point, BSON and GridFS benchmarks are not implemented because we do not own the Rust BSON library, and GridFS has not been implemented
in the driver.

Also note that the parallel benchmarks are implemented to mirror the C++ driver's interpretation of the spec.
