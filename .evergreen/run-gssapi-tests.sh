#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

echo "Running MONGODB-GSSAPI authentication tests"

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

# Source the drivers/atlas_connect secrets, where GSSAPI test values are held
source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

# Authenticate the user principal in the KDC before running the integration test
echo "$SASL_PASS" | kinit -p $PRINCIPAL
klist

FEATURE_FLAGS+=("gssapi-auth")

set +o errexit

cargo_test test::auth::gssapi
cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

exit $CARGO_RESULT
