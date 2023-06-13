#!/bin/bash

set -o errexit

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    OPTIONS=""
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    OPTIONS=" --no-default-features --features async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

source ./.evergreen/env.sh
RUST_BACKTRACE=1 cargo test atlas_connectivity ${OPTIONS}