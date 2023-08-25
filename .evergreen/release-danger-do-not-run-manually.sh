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

if [[ -z "$TAG" ]]; then
  echo >&2 "\$TAG must be set to the git tag of the release"
  exit 1
fi

if [[ -z "$TOKEN" ]]; then
  echo >&2 "\$TOKEN must be set to the crates.io authentication token"
  exit 1
fi

git fetch origin tag $TAG --no-tags
git checkout $TAG

source ./.evergreen/env.sh

cargo publish --token $TOKEN
