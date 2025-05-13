#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

# Install the MSRV and generate a new lockfile with MSRV-compatible dependencies.
if [ "$RUST_VERSION" != "" ]; then
  rustup toolchain install $RUST_VERSION
  TOOLCHAIN="+${RUST_VERSION}"
  # Remove the local git dependencies for bson and mongocrypt, which don't work properly with the MSRV resolver.
  sed -i "s/bson =.*/bson = \"2\"/" Cargo.toml
  sed -i "s/mongocrypt =.*/mongocrypt = { version = \"0.2\", optional = true }/" Cargo.toml
  CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback cargo +nightly -Zmsrv-policy generate-lockfile
fi

# Test with default features.
cargo $TOOLCHAIN build

# Test with all features.
cargo $TOOLCHAIN build --all-features

# Test with no default features.
cargo $TOOLCHAIN build --no-default-features --features compat-3-0-0,rustls-tls
