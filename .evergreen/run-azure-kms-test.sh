#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

AZUREKMS_TOOLS=$DRIVERS_TOOLS/.evergreen/csfle/azurekms/

set +o xtrace
source ${AZUREKMS_TOOLS}/secrets-export.sh
set -o xtrace

mkdir azurekms_remote
cp -r $MONGOCRYPT_LIB_DIR azurekms_remote

echo "Building test ... begin"
cargo test get_exe_name --features in-use-encryption,azure-kms
cp $(cat driver/exe_name.txt) azurekms_remote/test-exe
echo "Building test ... end"

tar czf azurekms_remote.tgz azurekms_remote
AZUREKMS_SRC=azurekms_remote.tgz \
  AZUREKMS_DST="." \
  $AZUREKMS_TOOLS/copy-file.sh
AZUREKMS_CMD="tar xvf azurekms_remote.tgz" $AZUREKMS_TOOLS/run-command.sh
AZUREKMS_CMD="RUST_BACKTRACE=1 LD_LIBRARY_PATH=./azurekms_remote/lib ./azurekms_remote/test-exe on_demand_azure::success -- --no-capture" \
  $AZUREKMS_TOOLS/run-command.sh
