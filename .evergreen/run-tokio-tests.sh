#!/bin/sh

set -o errexit

. ~/.cargo/env

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="-- --test-threads=1"
fi

FEATURE_FLAGS="zstd-compression,snappy-compression,zlib-compression"

echo "cargo test options: ${OPTIONS}"
echo "features flags: ${FEATURE_FLAGS}"

RUST_BACKTRACE=1 cargo test --features $FEATURE_FLAGS $OPTIONS
