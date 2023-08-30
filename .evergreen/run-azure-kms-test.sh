#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

AZUREKMS_TOOLS=$DRIVERS_TOOLS/.evergreen/csfle/azurekms/

mkdir azurekms_remote
cp -r $MONGOCRYPT_LIB_DIR azurekms_remote

pushd .evergreen/azure-kms-test
cargo build
popd
cp .evergreen/azure-kms-test/target/debug/azure-kms-test azurekms_remote

tar czf azurekms_remote.tgz azurekms_remote
AZUREKMS_SRC=azurekms_remote.tgz \
  AZUREKMS_DST="." \
  $AZUREKMS_TOOLS/copy-file.sh
AZUREKMS_CMD="tar xvf azurekms_remote.tgz" $AZUREKMS_TOOLS/run-command.sh
AZUREKMS_CMD="LD_LIBRARY_PATH=./azurekms_remote/lib KEY_NAME='${AZUREKMS_KEY_NAME}' KEY_VAULT_ENDPOINT='${AZUREKMS_KEY_VAULT_ENDPOINT}' ./azurekms_remote/azure-kms-test" \
  $AZUREKMS_TOOLS/run-command.sh
