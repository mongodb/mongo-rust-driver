#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

rm -rf test_files && mkdir test_files
cp ${TEST_FILES}/* test_files

export RUSTFLAGS="-C target-feature=+crt-static"
cargo test ${BUILD_FEATURES} --target x86_64-unknown-linux-gnu get_exe_name
TEST_BINARY=$(cat exe_name.txt)
TEST_TARBALL="/tmp/mongo-rust-driver.tar.gz"
tar czvf ${TEST_TARBALL} ${TEST_BINARY} ./.evergreen test_files

cat <<EOT > static-test-tarball-expansion.yml
STATIC_TEST_BINARY: ${TEST_BINARY}
STATIC_TEST_TARBALL: ${TEST_TARBALL}
EOT
