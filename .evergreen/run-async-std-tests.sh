#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

echo "cargo test options: ${OPTIONS}"

RUST_BACKTRACE=1 cargo test --no-default-features --features async-std-runtime $OPTIONS | tee async-tests.json
cat async-tests.json | cargo2junit > results.xml
