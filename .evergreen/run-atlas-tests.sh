#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

CARGO_OPTIONS+=("--ignore-default-filter")

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

set +o errexit

# Create client certificate file from base64 encoded secret:
mkdir -p .secrets
chmod 700 .secrets
echo "${ATLAS_X509_DEV_CERT_BASE64}" | base64 --decode > .secrets/clientcert.pem
ATLAS_X509_DEV_WITH_CERT="${ATLAS_X509_DEV}&tlsCertificateKeyFile=.secrets/clientcert.pem"
export ATLAS_X509_DEV_WITH_CERT

cargo_test test::atlas_connectivity

exit $CARGO_RESULT
