# Rust Driver Benchmark Suite

This suite implements the benchmarks described in this (spec)[https://github.com/mongodb/specifications/blob/master/source/benchmarking/benchmarking.rst]

In order to run the microbenchmarks, first run `etc/microbenchmark-test-data.sh` to download the data.

In order to run specific tests, just specify their names as arguments. If run with no arguments,
all benchmarks will be run. Also, remember to run in release
e.g. `cargo run --release BSONBench MultiBench

Full list of options:
SingleBench
MultiBench
ParallelBench

Note: make sure you run both the download script and the microbenchmarks binary from the project root.

See the spec for details on these benchmarks.

Note that in order to compare against the other drivers, an inMemory mongod instance should be 
used.

At this point, BSON and GridFS benchmarks are not implemented because we do not
own the Rust BSON library, and GridFS has not been implemented in the driver.

Also note that the parallel benchmarks are implemented to mirror the C++ driver's interpretation of the spec.
