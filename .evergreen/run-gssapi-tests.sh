#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

echo "Running MONGODB-GSSAPI authentication tests"

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("gssapi-auth")

set +o errexit

cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

exit $CARGO_RESULT
