#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

unset INDEX_MANAGEMENT_TEST_UNIFIED
export INDEX_MANAGEMENT_TEST_PROSE=1

set +o errexit

cargo_test test::spec::index_management

exit ${CARGO_RESULT}