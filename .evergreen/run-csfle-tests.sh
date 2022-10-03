#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

FEATURE_FLAGS="openssl-tls"
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

echo "cargo test options: ${DEFAULT_FEATURES} --features ${FEATURE_FLAGS} ${OPTIONS}"

CARGO_RESULT=0

cargo_test() {
    RUST_BACKTRACE=1 \
        cargo test ${DEFAULT_FEATURES} --features $FEATURE_FLAGS $1 $OPTIONS | cargo2junit
    (( CARGO_RESULT = $CARGO_RESULT || $? ))
}

set +o errexit

cargo_test test::csfle > results.xml

exit $CARGO_RESULT
