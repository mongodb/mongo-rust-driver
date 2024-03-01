#!/bin/bash

. ${PREPARE_SHELL}

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("sync")

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test sync results.xml

exit $CARGO_RESULT
