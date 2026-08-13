#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("${STANDARD_FEATURES[@]}")

add_conditional_features

echo "cargo test options: $(cargo_test_options)"

set +o errexit

if [ "Windows_NT" == "$OS" ]; then
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

cargo_test ""

exit $CARGO_RESULT
