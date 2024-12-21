#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

mkdir -p artifacts

# Function to run fuzzer and collect crashes
run_fuzzer() {
    target=$1
    echo "Running fuzzer for $target"
    # Run fuzzer and redirect crashes to artifacts directory
    RUST_BACKTRACE=1 cargo +nightly fuzz run $target -- \
        -rss_limit_mb=4096 \
        -max_total_time=360 \
        -artifact_prefix=artifacts/ \
        -print_final_stats=1
}

run_fuzzer header_length
