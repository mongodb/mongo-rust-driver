#!/bin/bash

set -o errexit

# source ./.evergreen/configure-rust.sh
source ./.evergreen/feature-combinations.sh

# Pin clippy to the latest version. This should be updated when new versions of Rust are released.
CLIPPY_VERSION=1.61.0

rustup install $CLIPPY_VERSION

for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
    cargo +$CLIPPY_VERSION clippy --all-targets ${FEATURE_COMBINATIONS[$i]}  -p mongodb -- -D warnings
done
