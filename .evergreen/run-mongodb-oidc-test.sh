#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV=${OIDC_ENV:-"test"}

if [ $OIDC_ENV == "test" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

elif [ $OIDC_ENV == "azure" ]; then
    source ./env.sh

else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"
export OIDC="oidc"

cargo nextest run test::spec::oidc --profile
RESULT=$?
cp target/nextest/ci/junit.xml results.xml
exit $RESULT
