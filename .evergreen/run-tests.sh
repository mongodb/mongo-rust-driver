#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("tracing-unstable")

if [ "$OPENSSL" = true ]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

if [ "$ZSTD" = true ]; then
  FEATURE_FLAGS+=("zstd-compression")
fi

if [ "$ZLIB" = true ]; then
  FEATURE_FLAGS+=("zlib-compression")
fi

if [ "$SNAPPY" = true ]; then
  FEATURE_FLAGS+=("snappy-compression")
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
