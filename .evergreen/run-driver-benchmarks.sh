#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

FEATURES=""

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURES="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURES="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

cd benchmarks
cargo run \
      --release \
      --no-default-features \
      --features ${FEATURES} \
      -- --output="../benchmark-results.json" --driver

cat ../benchmark-results.json
