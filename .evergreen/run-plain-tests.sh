#!/bin/bash

set -o errexit
set -o xtrace

source .evergreen/env.sh
source .evergreen/cargo-test.sh

set +o errexit

MONGO_PLAIN_AUTH_TEST=1 cargo_test plain

exit $CARGO_RESULT
