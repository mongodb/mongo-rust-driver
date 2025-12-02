#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("${STANDARD_FEATURES[@]}")

add_conditional_features

if [ "$USE_NEXTEST_ARCHIVE" = true ]; then
  # Feature flags are set when the archive is built
  WORKSPACE_ROOT="$(pwd)"
  if [ "Windows_NT" == "$OS" ]; then
    WORKSPACE_ROOT="$(cygpath -w ${WORKSPACE_ROOT})"
  fi
  FEATURE_FLAGS=()
  CARGO_OPTIONS+=("--archive-file" "nextest-archive.tar.zst" "--workspace-remap" "${WORKSPACE_ROOT}")
fi

echo "cargo test options: $(cargo_test_options)"

set +o errexit

if [ "Windows_NT" == "$OS" ]; then
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

cargo_test ""

exit $CARGO_RESULT
