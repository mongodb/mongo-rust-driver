#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

export MONGO_ATLAS_TESTS=1

. "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

set +x
export MONGO_ATLAS_FREE_TIER_REPL_URI="${atlas_free}"
export MONGO_ATLAS_FREE_TIER_REPL_URI_SRV="${atlas_srv_free}"

echo uri hash:
echo "${MONGO_ATLAS_FREE_TIER_REPL_URI}" | shasum
echo srv uri hash:
echo "${MONGO_ATLAS_FREE_TIER_REPL_URI_SRV}" | shasum
set -x

set +o errexit

cargo_test atlas_free_tier_repl_set results.xml

exit $CARGO_RESULT
