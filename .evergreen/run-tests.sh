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
export INDEX_MANAGEMENT_TEST_UNIFIED=1

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test "" results.xml

# cargo-nextest doesn't support doc tests
RUST_BACKTRACE=1 cargo test --doc $(cargo_test_options)
((CARGO_RESULT = ${CARGO_RESULT} || $?))

exit $CARGO_RESULT
