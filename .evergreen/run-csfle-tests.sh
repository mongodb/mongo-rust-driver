#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

FEATURE_FLAGS="csfle,${TLS_FEATURE}"
OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

if [ "$OS" = "Windows_NT" ]; then
    export CSFLE_TLS_CERT_DIR=$(cygpath ${CSFLE_TLS_CERT_DIR} --windows)
    export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
    export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

echo "cargo test options: --features ${FEATURE_FLAGS} ${OPTIONS}"

CARGO_RESULT=0

cargo_test() {
    RUST_BACKTRACE=1 \
        cargo test --features ${FEATURE_FLAGS} $1 ${OPTIONS} | cargo2junit
    (( CARGO_RESULT = ${CARGO_RESULT} || $? ))
}

set +o errexit

cargo_test test::csfle > results.xml

exit ${CARGO_RESULT}
