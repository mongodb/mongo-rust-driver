#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

# Install the MSRV and generate a new lockfile with MSRV-compatible dependencies.
if [ "$RUST_VERSION" != "" ]; then
  rustup toolchain install $RUST_VERSION
  TOOLCHAIN="+${RUST_VERSION}"
  CARGO_RESOLVER_INCOMPATIBLE_RUST_VERSIONS=fallback cargo +nightly -Zmsrv-policy generate-lockfile
fi

# Test with default features.
cargo $TOOLCHAIN build

# Test with all features.
if [ "$RUST_VERSION" != "" ]; then
  cargo $TOOLCHAIN build --features openssl-tls,sync,aws-auth,zlib-compression,zstd-compression,snappy-compression,in-use-encryption,tracing-unstable
else
  cargo $TOOLCHAIN build --all-features
fi

# Test with no default features.
if [ "$RUST_VERSION" != "" ]; then
  cargo $TOOLCHAIN build --no-default-features --features compat-3-3-0,rustls-tls
else
  cargo $TOOLCHAIN build --no-default-features --features compat-3-3-0,bson-3,rustls-tls
fi
