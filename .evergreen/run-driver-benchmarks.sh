#!/bin/sh

set -o errexit

. ~/.cargo/env

FEATURES=""

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURES="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURES="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

export MIN_EXECUTION_TIME=1
export TARGET_ITERATION_COUNT=3

cd benchmarks
cargo run \
      --release \
      --no-default-features \
      --features ${FEATURES} \
      -- --output="../benchmark-results.json" --single --multi --parallel

cat ../benchmark-results.json
