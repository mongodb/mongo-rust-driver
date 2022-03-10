#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

FEATURE_FLAGS="zstd-compression,snappy-compression,zlib-compression"

echo "cargo test options: --features $FEATURE_FLAGS ${OPTIONS}"

set +o errexit
CARGO_RESULT=0

RUST_BACKTRACE=1 cargo test --features $FEATURE_FLAGS $OPTIONS | tee results.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat results.json | cargo2junit > async-tests.xml
RUST_BACKTRACE=1 cargo test sync --features tokio-sync,$FEATURE_FLAGS $OPTIONS | tee sync-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-tests.json | cargo2junit > sync-tests.xml
RUST_BACKTRACE=1 cargo test --doc sync --features tokio-sync,$FEATURE_FLAGS $OPTIONS | tee sync-doc-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-doc-tests.json | cargo2junit > sync-doc-tests.xml

junit-report-merger results.xml async-tests.xml sync-tests.xml sync-doc-tests.xml

exit $CARGO_RESULT