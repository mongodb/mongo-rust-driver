#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

# Supported/used environment variables:
#  MONGODB_URI    Set the URI, including an optional username/password to use
#                 to connect to the server via MONGODB-AWS authentication
#                 mechanism.

echo "Running MONGODB-AWS authentication tests"
# ensure no secrets are printed in log files
set +x

# load the script
shopt -s expand_aliases # needed for `urlencode` alias
[ -s "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh" ] && source "${PROJECT_DIRECTORY}/prepare_mongodb_aws.sh"

MONGODB_URI=${MONGODB_URI:-"mongodb://localhost"}
MONGODB_URI="${MONGODB_URI}/aws?authMechanism=MONGODB-AWS"
if [[ -n ${SESSION_TOKEN} ]]; then
  MONGODB_URI="${MONGODB_URI}&authMechanismProperties=AWS_SESSION_TOKEN:${SESSION_TOKEN}"
fi

export MONGODB_URI="$MONGODB_URI"

if [ "$ASSERT_NO_URI_CREDS" = "true" ]; then
  if echo "$MONGODB_URI" | grep -q "@"; then
    echo "MONGODB_URI unexpectedly contains user credentials!"
    exit 1
  fi
fi

# show test output
set -x

set -o errexit

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("aws-auth")

set +o errexit

cargo_test auth_aws auth_aws.xml
cargo_test lambda_examples::auth::test_handler lambda_handler.xml
cargo_test spec::auth spec.xml
cargo_test uri_options uri_options.xml
cargo_test connection_string connection_string.xml

junit-report-merger results.xml auth_aws.xml lambda_handler.xml spec.xml uri_options.xml connection_string.xml

exit $CARGO_RESULT
