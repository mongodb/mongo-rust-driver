#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

echo "Running MONGODB-OIDC authentication tests"

# Make sure DRIVERS_TOOLS is set.
if [ -z "$DRIVERS_TOOLS" ]; then
    echo "Must specify DRIVERS_TOOLS"
    exit 1
fi

source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

set +o errexit

cargo_test test::spec::oidc_skip_ci::basic
cargo_test test::spec::auth::run_unified

exit $CARGO_RESULT
