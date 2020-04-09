#!/bin/sh

set -o errexit

. ~/.cargo/env
RUST_BACKTRACE=1 cargo test --no-default-features --features async-std-runtime
RUST_BACKTRACE=1 cargo test sync --no-default-features --features sync
RUST_BACKTRACE=1 cargo test --doc --no-default-features --features sync
