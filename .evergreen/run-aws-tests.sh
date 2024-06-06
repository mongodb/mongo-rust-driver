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

cargo_test auth_aws auth_aws.xml
cargo_test lambda_examples::auth::test_handler lambda_handler.xml
cargo_test spec::auth spec.xml
cargo_test uri_options uri_options.xml
cargo_test connection_string connection_string.xml

merge-junit -o results.xml auth_aws.xml lambda_handler.xml spec.xml uri_options.xml connection_string.xml

exit $CARGO_RESULT
