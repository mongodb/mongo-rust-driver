#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh
# Pin clippy to the lastest version. This should be updated when new versions of Rust are released.
rustup default 1.59.0

source ./.evergreen/feature-combinations.sh

for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
    cargo clippy --all-targets ${FEATURE_COMBINATIONS[$i]}  -p mongodb -- -D warnings
done
