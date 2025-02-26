#!/bin/bash

set +x          # Disable debug trace
set -o errexit  # Exit the script with error if any of the commands fail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

echo "Running MONGODB-OIDC authentication tests"

OIDC_ENV=${OIDC_ENV:-"test"}

if [ $OIDC_ENV == "test" ]; then
    # Make sure DRIVERS_TOOLS is set.
    if [ -z "$DRIVERS_TOOLS" ]; then
        echo "Must specify DRIVERS_TOOLS"
        exit 1
    fi
    source ${DRIVERS_TOOLS}/.evergreen/auth_oidc/secrets-export.sh

    cargo_test test::spec::oidc::basic
    RESULT=$CARGO_RESULT
elif [ $OIDC_ENV == "azure" ]; then
    source ./env.sh

    $TEST_FILE test::spec::oidc::azure --nocapture
    RESULT=$?
elif [ $OIDC_ENV == "gcp" ]; then
    source ./secrets-export.sh

    $TEST_FILE test::spec::oidc::gcp --nocapture
    RESULT=$?
elif [ $OIDC_ENV == "k8s" ]; then
    $TEST_FILE test::spec::oidc::k8s --nocapture
    RESULT=$?
else
    echo "Unrecognized OIDC_ENV '${OIDC_ENV}'"
    exit 1
fi

exit $RESULT
