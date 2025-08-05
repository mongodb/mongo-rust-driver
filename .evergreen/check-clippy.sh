#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

# Pin clippy to the latest version. This should be updated when new versions of Rust are released.
CLIPPY_VERSION=1.88.0

rustup install $CLIPPY_VERSION

# Check with default features.
cargo +$CLIPPY_VERSION clippy --all-targets -p mongodb -- -D warnings

# Check with all features.
cargo +$CLIPPY_VERSION clippy --all-targets --all-features -p mongodb -- -D warnings
