#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

# Install the MSRV and pin dependencies who have bumped their MSRVs to > ours in recent releases.
if [ "$RUST_VERSION" != "" ]; then
  rustup toolchain install $RUST_VERSION
  TOOLCHAIN="+${RUST_VERSION}"
  patch Cargo.toml .evergreen/MSRV-Cargo.toml.diff
fi

source ./.evergreen/feature-combinations.sh

# Test compilation with all feature combinations.
for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
  cargo $TOOLCHAIN build ${FEATURE_COMBINATIONS[$i]}
done
