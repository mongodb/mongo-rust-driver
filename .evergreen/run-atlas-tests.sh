#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

export MONGO_ATLAS_TESTS=1

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

set +o errexit

cargo_test atlas_connectivity results.xml

exit $CARGO_RESULT
