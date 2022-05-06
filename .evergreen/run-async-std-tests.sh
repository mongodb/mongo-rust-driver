#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time --test-threads 1"

echo "cargo test options: ${OPTIONS}"

set +o errexit
CARGO_RESULT=0

RUST_BACKTRACE=1 cargo test --no-default-features --features async-std-runtime,${TLS_FEATURE} $OPTIONS | tee async-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat async-tests.json | cargo2junit > async-tests.xml
RUST_BACKTRACE=1 cargo test sync --no-default-features --features sync,${TLS_FEATURE} $OPTIONS | tee sync-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-tests.json | cargo2junit > sync-tests.xml
RUST_BACKTRACE=1 cargo test --doc sync --no-default-features --features sync,${TLS_FEATURE} $OPTIONS | tee sync-doc-tests.json
(( CARGO_RESULT = CARGO_RESULT || $? ))
cat sync-doc-tests.json | cargo2junit > sync-doc-tests.xml

junit-report-merger results.xml async-tests.xml sync-tests.xml sync-doc-tests.xml

exit $CARGO_RESULT
