#!/bin/sh

set -o errexit

FEATURE_FLAGS=snappy-compression,zlib-compression

if [[ $RUST_VERSION == "nightly" ]]; then
    FEATURE_FLAGS=$FEATURE_FLAGS,zstd-compression
fi

. ~/.cargo/env
cargo clippy --all-targets -p mongodb -- -D warnings
cargo clippy --all-targets -p mongodb --features $FEATURE_FLAGS -- -D warnings
cargo clippy --all-targets --no-default-features --features async-std-runtime -p mongodb -- -D warnings
cargo clippy --all-targets --no-default-features --features sync -p mongodb -- -D warnings
