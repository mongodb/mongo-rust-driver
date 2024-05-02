#!/bin/bash

set -o errexit
set +x

. ${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh drivers/rust
rm secrets-export.sh

PAPERTRAIL_PRODUCT="rust-driver"
if [[ "${DRY_RUN}" == "yes" ]]; then
  PAPERTRAIL_PRODUCT="rust-driver-testing"
fi

cat <<EOT >papertrail-expansion.yml
PAPERTRAIL_KEY_ID: "${PAPERTRAIL_KEY_ID}"
PAPERTRAIL_SECRET_KEY: "${PAPERTRAIL_SECRET_KEY}"
PAPERTRAIL_PRODUCT: "${PAPERTRAIL_PRODUCT}"
EOT
