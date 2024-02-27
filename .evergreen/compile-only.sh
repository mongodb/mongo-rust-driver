#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

# Install the MSRV and pin dependencies who have bumped their MSRVs to > ours in recent releases.
if [ "$RUST_VERSION" != "" ]; then
  rustup toolchain install $RUST_VERSION
  TOOLCHAIN="+${RUST_VERSION}"
  cp .evergreen/MSRV-Cargo.lock Cargo.lock
fi

# Test with default features.
cargo $TOOLCHAIN build

# Test with all features.
cargo $TOOLCHAIN build --all-features
