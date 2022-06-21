#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh
rustup update $RUST_VERSION

# pin dependencies who have bumped their MSRVs to > ours in recent releases.
if [  "$MSRV" = "true" ]; then
    cp .evergreen/MSRV-Cargo.lock Cargo.lock
fi

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURE_FLAGS=snappy-compression,zlib-compression

    # Zstd requires Rust version 1.54
    if [[ $RUST_VERSION == "nightly" ]]; then
        FEATURE_FLAGS=$FEATURE_FLAGS,zstd-compression
    fi

    rustup run $RUST_VERSION cargo build --features $FEATURE_FLAGS
    rustup run $RUST_VERSION cargo build --features tokio-sync,$FEATURE_FLAGS

elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    rustup run $RUST_VERSION cargo build --no-default-features --features async-std-runtime
    rustup run $RUST_VERSION cargo build --no-default-features --features sync
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi
