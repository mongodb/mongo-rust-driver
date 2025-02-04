#!/bin/bash

set -o errexit
set -o xtrace

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

set +o errexit

cargo_test plain_auth

exit $CARGO_RESULT
