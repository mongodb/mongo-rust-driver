#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("tracing-unstable")

if [ "$ASYNC_STD" = true ]; then
  CARGO_OPTIONS+=("--no-default-features")
  FEATURE_FLAGS+=("async-std-runtime")
fi

if [ "$OPENSSL" = true ]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

if [ "$COMPRESSION" = true ]; then
  FEATURE_FLAGS+=("snappy-compression", "zlib-compression", "zstd-compression")
fi

export SESSION_TEST_REQUIRE_MONGOCRYPTD=true

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test "" results.xml

exit $CARGO_RESULT
