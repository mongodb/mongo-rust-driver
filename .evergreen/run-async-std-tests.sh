#!/bin/sh

set -o errexit

. ~/.cargo/env

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="-- --test-threads=1"
fi

echo "cargo test options: ${OPTIONS}"

RUST_BACKTRACE=1 cargo test --no-default-features --features async-std-runtime $OPTIONS
RUST_BACKTRACE=1 cargo test sync --no-default-features --features sync $OPTIONS
RUST_BACKTRACE=1 cargo test --doc sync --no-default-features --features sync $OPTIONS
