#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV=${OIDC_ENV:-"test"}

export TEST_AUTH_OIDC=1
export COVERAGE=1
export AUTH="auth"

if [ $OIDC_ENV == "test" ]; then

    source .evergreen/env.sh
    source .evergreen/cargo-test.sh
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

    cargo nextest run test::spec::oidc::basic --no-capture --profile ci
    RESULT=$?
    cp target/nextest/ci/junit.xml results.xml
elif [ $OIDC_ENV == "azure" ]; then
    source .evergreen/env.sh
    source .evergreen/cargo-test.sh
    source ./env.sh

    cargo nextest run test::spec::oidc::azure --no-capture --profile ci --features=azure-oidc
    RESULT=$?
    cp target/nextest/ci/junit.xml results.xml
elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh

    ./target/debug/deps/mongodb-*  test::spec::oidc::gcp --nocapture
    RESULT=$?
else
    echo "Unrecognized OIDC_ENV $OIDC_ENV"
    exit 1
fi

exit $RESULT
