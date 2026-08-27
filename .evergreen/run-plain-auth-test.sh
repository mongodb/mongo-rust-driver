#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

echo "Running PLAIN authentication test"

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/enterprise_auth

set +o errexit

cargo_test plain_auth_skip_local

exit $CARGO_RESULT
