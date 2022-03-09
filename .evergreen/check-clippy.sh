#!/bin/sh

set -o errexit

. ~/.cargo/env
# Pin clippy to the lastest version. This should be updated when new versions of Rust are released.
rustup default 1.59.0
cargo clippy --all-targets -p mongodb -- -D warnings
# check clippy with compressors separately
cargo clippy --all-targets -p mongodb --features zstd-compression,snappy-compression,zlib-compression -- -D warnings
cargo clippy --all-targets --no-default-features --features async-std-runtime -p mongodb -- -D warnings
cargo clippy --all-targets --no-default-features --features sync -p mongodb -- -D warnings
cargo clippy --all-targets --features tokio-sync -p mongodb -- -D warnings
