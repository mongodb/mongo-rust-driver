#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh
rustup update $RUST_VERSION

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURE_FLAGS=snappy-compression,zlib-compression

    # Zstd requires Rust version 1.54
    if [[ $RUST_VERSION == "nightly" ]]; then
        FEATURE_FLAGS=$FEATURE_FLAGS,zstd-compression
    fi

    rustup run $RUST_VERSION cargo build --features $FEATURE_FLAGS
    rustup run $RUST_VERSION cargo build --features tokio-sync,$FEATURE_FLAGS

elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    # v2.1 of async-global-executor bumped its MSRV to 1.59, so we need a Cargo.lock
    # pinning to v2.0 to build with our MSRV.
    if [  "$MSRV" = "true" ]; then
        cp .evergreen/MSRV-Cargo.lock Cargo.lock
    fi

    rustup run $RUST_VERSION cargo build --no-default-features --features async-std-runtime
    rustup run $RUST_VERSION cargo build --no-default-features --features sync
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi
