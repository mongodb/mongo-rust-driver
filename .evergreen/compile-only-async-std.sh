#!/bin/sh

set -o errexit

source ./.evergreen/env.sh

rustup run $RUST_VERSION cargo build --no-default-features --features async-std-runtime
rustup run $RUST_VERSION cargo build --no-default-features --features sync
