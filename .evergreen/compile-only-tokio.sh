#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

# Enable snappy, zlib unconditionally
FEATURE_FLAGS=snappy-compression,zlib-compression

# Zstd requires Rust version 1.54
if [[ $RUST_VERSION == "nightly" ]]; then
    FEATURE_FLAGS=$FEATURE_FLAGS,zstd-compression
fi

rustup run $RUST_VERSION cargo build --features $FEATURE_FLAGS
rustup run $RUST_VERSION cargo build --features tokio-sync,$FEATURE_FLAGS
