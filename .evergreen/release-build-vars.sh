#!/bin/bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set +x

CRATE_VERSION=$(cargo metadata --format-version=1 --no-deps | jq --raw-output '.packages[0].version')

. ${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh drivers/rust
rm secrets-export.sh

PAPERTRAIL_PRODUCT="rust-driver"
TEST_PREFIX=""
if [[ "${DRY_RUN:-}" == "yes" ]]; then
  PAPERTRAIL_PRODUCT="rust-driver-testing"
  TEST_PREFIX="testing-"
fi

cat <<EOT >release-expansion.yml
CARGO_REGISTRY_TOKEN: "${CARGO_REGISTRY_TOKEN}"
CRATE_VERSION: "${CRATE_VERSION}"
PAPERTRAIL_KEY_ID: "${PAPERTRAIL_KEY_ID}"
PAPERTRAIL_SECRET_KEY: "${PAPERTRAIL_SECRET_KEY}"
PAPERTRAIL_PRODUCT: "${PAPERTRAIL_PRODUCT}"
GARASIGN_USERNAME: "${GARASIGN_USERNAME}"
GARASIGN_PASSWORD: "${GARASIGN_PASSWORD}"
S3_UPLOAD_AWS_KEY: "${S3_UPLOAD_AWS_KEY}"
S3_UPLOAD_AWS_SECRET: "${S3_UPLOAD_AWS_SECRET}"
TEST_PREFIX: "${TEST_PREFIX}"
EOT
