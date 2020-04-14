#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo clippy --all-targets -p mongodb -- -D warnings
cargo clippy --all-targets --no-default-features --features async-std-runtime -p mongodb -- -D warnings
cargo clippy --all-targets --no-default-features --features sync -p mongodb -- -D warnings
