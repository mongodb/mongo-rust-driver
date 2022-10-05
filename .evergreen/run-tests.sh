#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time"
if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

FEATURE_FLAGS="zstd-compression,snappy-compression,zlib-compression,${TLS_FEATURE}"

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    ASYNC_FEATURE_FLAGS=${FEATURE_FLAGS}
    SYNC_FEATURE_FLAGS="tokio-sync,${FEATURE_FLAGS}"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    OPTIONS="--no-default-features ${OPTIONS}"
    ASYNC_FEATURE_FLAGS="async-std-runtime,${FEATURE_FLAGS}"
    SYNC_FEATURE_FLAGS="sync,${FEATURE_FLAGS}"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

echo "cargo test options: --features ${ASYNC_FEATURE_FLAGS} ${OPTIONS}"

set +o errexit
CARGO_RESULT=0

RUST_BACKTRACE=1 cargo test --features $ASYNC_FEATURE_FLAGS $OPTIONS | tee results.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat results.json | cargo2junit > async-tests.xml
RUST_BACKTRACE=1 cargo test sync --features $SYNC_FEATURE_FLAGS $OPTIONS | tee sync-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-tests.json | cargo2junit > sync-tests.xml
RUST_BACKTRACE=1 cargo test --doc sync --features $SYNC_FEATURE_FLAGS $OPTIONS | tee sync-doc-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-doc-tests.json | cargo2junit > sync-doc-tests.xml

junit-report-merger results.xml async-tests.xml sync-tests.xml sync-doc-tests.xml

exit $CARGO_RESULT