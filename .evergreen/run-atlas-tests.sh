#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

set +o errexit

cargo_test test::atlas_connectivity

exit $CARGO_RESULT
