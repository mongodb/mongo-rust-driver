#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

if [ "$ASYNC_STD" = true ]; then
    CARGO_OPTIONS+=("--no-default-features")
    FEATURE_FLAGS+=("sync")
else
    FEATURE_FLAGS+=("tokio-sync")
fi

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test sync > results.xml

exit $CARGO_RESULT