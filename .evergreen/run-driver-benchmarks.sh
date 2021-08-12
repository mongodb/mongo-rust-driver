#!/bin/sh

set -o errexit

FEATURES=""

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURES="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURES="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

pushd benchmarks

export MIN_EXECUTION_TIME=1
export TARGET_ITERATION_COUNT=3

cargo run \
      --release \
      --no-default-features \
      --features ${FEATURES} \
      -- --output="../benchmark-results.json" --single --multi --parallel

popd
