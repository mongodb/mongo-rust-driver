#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("tracing-unstable" "cert-key-password" "opentelemetry")

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

echo "cargo test options: $(cargo_test_options)"

set +o errexit

if [ "Windows_NT" == "$OS" ]; then
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

cargo_test ""

# cargo-nextest doesn't support doc tests
RUST_BACKTRACE=1 cargo test --doc $(cargo_test_options)
((CARGO_RESULT = ${CARGO_RESULT} || $?))

exit $CARGO_RESULT
