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

if [[ -z "$CARGO_REGISTRY_TOKEN" ]]; then
  echo >&2 "\$CARGO_REGISTRY_TOKEN must be set to the crates.io authentication token"
  exit 1
fi

source ./.evergreen/env.sh

EXTRA=""
if [[ "${DRY_RUN}" == "yes" ]]; then
  EXTRA="--dry-run"
fi

cargo publish --package mongodb-internal-macros ${EXTRA}
cargo publish --package mongodb ${EXTRA}
