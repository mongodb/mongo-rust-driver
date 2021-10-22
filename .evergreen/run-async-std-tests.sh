#!/bin/sh

set -o errexit

. ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

echo "cargo test options: ${OPTIONS}"

RUST_BACKTRACE=1 cargo test --no-default-features --features async-std-runtime $OPTIONS | tee async-tests.json
cat async-tests.json | cargo2junit > async-tests.xml
RUST_BACKTRACE=1 cargo test sync --no-default-features --features sync $OPTIONS | tee sync-tests.json
cat sync-tests.json | cargo2junit > sync-tests.xml
RUST_BACKTRACE=1 cargo test --doc sync --no-default-features --features sync $OPTIONS | tee sync-doc-tests.json
cat sync-doc-tests.json | cargo2junit > sync-doc-tests.xml

junit-report-merger results.xml async-tests.xml sync-tests.xml sync-doc-tests.xml
