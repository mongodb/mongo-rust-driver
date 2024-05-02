#!/bin/bash

# ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢
# # Danger!
#
# This script is used to publish a release of the driver to crates.io.
#
# Do not run it manually!
# ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠ ☢ ☠

# Disable tracing
set +x

set -o errexit

if [[ -z "$CRATES_IO_TOKEN" ]]; then
  echo >&2 "\$CRATES_IO_TOKEN must be set to the crates.io authentication token"
  exit 1
fi

source ./.evergreen/env.sh

EXTRA=""
if [[ "${DRY_RUN}" == "yes" ]]; then
  EXTRA="--dry-run"
fi

if [[ "${PACKAGE_ONLY}" == "yes" ]]; then
  cargo package --no-verify --allow-dirty
else
  cargo publish --token $CRATES_IO_TOKEN ${EXTRA}
fi
