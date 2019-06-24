# Rust Driver Benchmark Suite

This suite implements the benchmarks described in this (spec)[https://github.com/mongodb/specifications/blob/master/source/benchmarking/benchmarking.rst].

In order to run the microbenchmarks, first run `../etc/microbenchmark-test-data.sh` to download the data. Then run `cargo build --release` to
build benchmarks in release mode.

Note: make sure you run the download script and the microbenchmarks binary from the benchmark root (the directory containing this README).

See the spec for details on these benchmarks.

To execute all benchmarks, run `./target/release/rust-driver-bench` with a mongod instance running on port 27017 (or, you can specify a custom 
connection string by setting the `MONGODB_URI` environment variable).

Run `./target/release/rust-driver-bench --help` to see a full list of testing options.

Note that in order to compare against the other drivers, an inMemory mongod instance should be used.

At this point, BSON and GridFS benchmarks are not implemented because we do not own the Rust BSON library, and GridFS has not been implemented
in the driver.

Also note that the parallel benchmarks are implemented to mirror the C++ driver's interpretation of the spec.
