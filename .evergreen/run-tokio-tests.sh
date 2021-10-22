#!/bin/sh

set -o errexit

. ~/.cargo/env

OPTIONS="-- -Z unstable-options --format json --report-time"

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="$OPTIONS --test-threads=1"
fi

FEATURE_FLAGS="zstd-compression,snappy-compression,zlib-compression"

echo "cargo test options: --features $FEATURE_FLAGS ${OPTIONS}"

RUST_BACKTRACE=1 cargo test --features $FEATURE_FLAGS $OPTIONS | tee results.json
cat results.json | cargo2junit > results.xml
