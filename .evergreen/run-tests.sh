#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

use_single_thread
use_async_runtime

FEATURE_FLAGS+=("tracing-unstable" "${TLS_FEATURE}")

if [ "$SNAPPY_COMPRESSION_ENABLED" = true ]; then
	FEATURE_FLAGS+=("snappy-compression")
fi
if [ "$ZLIB_COMPRESSION_ENABLED" = true ]; then
	FEATURE_FLAGS+=("zlib-compression")
fi
if [ "$ZSTD_COMPRESSION_ENABLED" = true ]; then
	FEATURE_FLAGS+=("zstd-compression")
fi

SYNC_FEATURE=""
if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    SYNC_FEATURE="tokio-sync"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    SYNC_FEATURE="sync"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test > async-tests.xml
FEATURE_FLAGS+=("${SYNC_FEATURE}")
cargo_test sync > sync-tests.xml
CARGO_OPTIONS+=("--doc")
cargo_test sync > sync-doc-tests.xml

junit-report-merger results.xml async-tests.xml sync-tests.xml sync-doc-tests.xml

exit $CARGO_RESULT