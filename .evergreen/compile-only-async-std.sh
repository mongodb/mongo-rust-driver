#!/bin/sh

set -o errexit

. ~/.cargo/env

rustup run $RUST_VERSION cargo build --no-default-features --features async-std-runtime
rustup run $RUST_VERSION cargo build --no-default-features --features sync
