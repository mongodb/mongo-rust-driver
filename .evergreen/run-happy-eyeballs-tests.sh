#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

set +o errexit

cargo_test "test::happy_eyeballs"
exit $CARGO_RESULT