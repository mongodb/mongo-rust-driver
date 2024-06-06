#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set +x

CRATE_VERSION=$(cargo metadata --format-version=1 --no-deps | jq --raw-output '.packages[0].version')

. ${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh drivers/rust
rm secrets-export.sh

PAPERTRAIL_PRODUCT="rust-driver"
if [[ "${DRY_RUN:-}" == "yes" ]]; then
  PAPERTRAIL_PRODUCT="rust-driver-testing"
fi

cat <<EOT >release-expansion.yml
CRATE_VERSION: "${CRATE_VERSION}"
PAPERTRAIL_KEY_ID: "${PAPERTRAIL_KEY_ID}"
PAPERTRAIL_SECRET_KEY: "${PAPERTRAIL_SECRET_KEY}"
PAPERTRAIL_PRODUCT: "${PAPERTRAIL_PRODUCT}"
ARTIFACTORY_USERNAME: "${ARTIFACTORY_USERNAME}"
ARTIFACTORY_PASSWORD: "${ARTIFACTORY_PASSWORD}"
GARASIGN_USERNAME: "${GARASIGN_USERNAME}"
GARASIGN_PASSWORD: "${GARASIGN_PASSWORD}"
EOT
