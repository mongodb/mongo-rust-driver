#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

FEATURE_FLAGS="in-use-encryption-unstable,aws-auth,${TLS_FEATURE}"
OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

if [ "$OS" = "Windows_NT" ]; then
    export CSFLE_TLS_CERT_DIR=$(cygpath ${CSFLE_TLS_CERT_DIR} --windows)
    export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
    export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

export AWS_DEFAULT_REGION=us-east-1
. ${DRIVERS_TOOLS}/.evergreen/csfle/set-temp-creds.sh

echo "cargo test options: --features ${FEATURE_FLAGS} ${OPTIONS}"

CARGO_RESULT=0

cargo_test() {
    RUST_BACKTRACE=1 \
        cargo test --features ${FEATURE_FLAGS} $1 ${OPTIONS} | grep -v '{"t":' | cargo2junit
    (( CARGO_RESULT = ${CARGO_RESULT} || $? ))
}

set +o errexit

cargo_test test::csfle > prose.xml
cargo_test test::spec::client_side_encryption > spec.xml

(
    unset AWS_ACCESS_KEY_ID
    unset AWS_SECRET_ACCESS_KEY
    cargo test test::csfle::on_demand_aws_failure > failure.xml
)

junit-report-merger results.xml prose.xml spec.xml failure.xml

exit ${CARGO_RESULT}
