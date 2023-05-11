#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/configure-rust.sh

set -o xtrace

AZUREKMS_TOOLS=$DRIVERS_TOOLS/.evergreen/csfle/azurekms/

pushd .evergreen/azure-kms-test
cargo build
AZUREKMS_SRC=target/debug/azure-kms-test \
    AZUREKMS_DST="." \
    $AZUREKMS_TOOLS/copy-file.sh
popd

AZUREKMS_CMD=azure-kms-test $AZUREKMS_TOOLS/run-command.sh