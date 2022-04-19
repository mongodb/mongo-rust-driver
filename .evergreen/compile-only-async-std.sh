#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

rustup run $RUST_VERSION cargo build --no-default-features --features async-std-runtime
rustup run $RUST_VERSION cargo build --no-default-features --features sync
