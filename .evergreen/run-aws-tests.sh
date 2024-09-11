#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

echo "Running MONGODB-AWS authentication tests"

cd $DRIVERS_TOOLS/.evergreen/auth_aws
. aws_setup.sh $AWS_AUTH_TYPE

set -o errexit

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("aws-auth")

set +o errexit

cargo_test auth_aws
cargo_test lambda_examples::auth::test_handler
cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

exit $CARGO_RESULT
