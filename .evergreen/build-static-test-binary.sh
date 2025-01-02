#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

export RUSTFLAGS="-C target-feature=+crt-static"
cargo test --features azure-oidc --target x86_64-unknown-linux-gnu get_exe_name -- --ignored
TEST_BINARY=$(cat exe_name.txt)
echo "STATIC_TEST_BINARY: ${TEST_BINARY}" > static-test-binary-expansion.yml