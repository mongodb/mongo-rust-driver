#!/usr/bin/env bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

export CSFLE_TLS_CERT_DIR="${DRIVERS_TOOLS}/.evergreen/x509gen"

CARGO_OPTIONS+=("--ignore-default-filter")

FLE_AZURE_USE_CORPORATE="YES" bash ${DRIVERS_TOOLS}/.evergreen/csfle/setup-secrets.sh
. ./secrets-export.sh

# Add mongodb binaries to path for mongocryptd
PATH=${PATH}:${DRIVERS_TOOLS}/mongodb/bin

# Always stop the CSFLE servers when this script exits
trap 'bash ${DRIVERS_TOOLS}/.evergreen/csfle/stop-servers.sh || true' EXIT
bash ${DRIVERS_TOOLS}/.evergreen/csfle/start-servers.sh

set -o xtrace
set +o errexit

TEST_OPTIONS=("--skip" "on_demand_aws::failure" "--skip" "custom_aws_credentials")
cargo_test test::csfle

# Unset variables for credential failure tests.
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
TEST_OPTIONS=()
cargo_test on_demand_aws::failure custom_aws_credentials

exit ${CARGO_RESULT}
