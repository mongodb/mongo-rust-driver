#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

FEATURE_FLAGS="zstd-compression,snappy-compression,zlib-compression"
DEFAULT_FEATURES=""

if [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURE_FLAGS="${FEATURE_FLAGS},async-std-runtime"
    DEFAULT_FEATURES="--no-default-features"
elif [ "$ASYNC_RUNTIME" != "tokio" ]; then
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

echo "cargo test options: ${DEFAULT_FEATURES} --features $FEATURE_FLAGS ${OPTIONS}"

CARGO_RESULT=0

cargo_test() {
    RUST_BACKTRACE=1 \
    SERVERLESS="serverless" \
        cargo test ${DEFAULT_FEATURES} --features $FEATURE_FLAGS $1 $OPTIONS | cargo2junit
    (( CARGO_RESULT = $CARGO_RESULT || $? ))
}

set +o errexit

cargo_test test::spec::crud > crud.xml
cargo_test test::spec::retryable_reads > retryable_reads.xml
cargo_test test::spec::retryable_writes > retryable_writes.xml
cargo_test test::spec::versioned_api > versioned_api.xml
cargo_test test::spec::sessions > sessions.xml
cargo_test test::spec::transactions > transactions.xml
cargo_test test::spec::load_balancers > load_balancers.xml
cargo_test test::cursor > cursor.xml

junit-report-merger results.xml crud.xml retryable_reads.xml retryable_writes.xml versioned_api.xml sessions.xml transactions.xml load_balancers.xml cursor.xml

exit $CARGO_RESULT
