#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

set -o xtrace

set +o errexit

cargo_test test::index_management::search_index

exit ${CARGO_RESULT}
