#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/sfp

mkdir -p .secrets
chmod 700 .secrets
echo "${SFP_ATLAS_X509_BASE64}" | base64 --decode > .secrets/sfp-cert.pem
export SFP_ATLAS_X509_CERT="$(pwd)/.secrets/sfp-cert.pem"

set +o errexit

CARGO_OPTIONS+=("--ignore-default-filter")
FEATURE_FLAGS+=("snappy-compression")

cargo_test atlas_sfp

exit $CARGO_RESULT
