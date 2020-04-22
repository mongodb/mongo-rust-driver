#!/bin/sh

set -o errexit

. ~/.cargo/env

if [ "$SINGLE_THREAD" = true ]; then
	OPTIONS="-- --test-threads=1"
fi

echo "cargo test options: ${OPTIONS}"

RUST_BACKTRACE=1 cargo test $OPTIONS
