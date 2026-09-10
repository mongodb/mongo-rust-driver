#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

GCP_KMS_TOOLS=$DRIVERS_TOOLS/.evergreen/csfle/gcpkms/

set +o xtrace
source $GCP_KMS_TOOLS/secrets-export.sh
set -o xtrace

mkdir test-contents
cp -r $MONGOCRYPT_LIB_DIR test-contents

echo "Building test ... begin"
cargo test get_exe_name --features in-use-encryption,gcp-kms
cp $(cat driver/exe_name.txt) test-contents/test-exe
echo "Building test ... end"

echo "Copying test contents ... begin"
tar czf test-contents.tgz test-contents
GCPKMS_SRC=test-contents.tgz GCPKMS_DST=$GCPKMS_INSTANCENAME: $GCP_KMS_TOOLS/copy-file.sh
echo "Copying test contents ... end"

echo "Untarring test contents ... begin"
GCPKMS_CMD="tar xf test-contents.tgz" $GCP_KMS_TOOLS/run-command.sh
echo "Untarring test contents ... end"

echo "Running test ... begin"
GCPKMS_CMD="RUST_BACKTRACE=1 LD_LIBRARY_PATH=./test-contents/lib ./test-contents/test-exe on_demand_gcp::success -- --no-capture" $GCP_KMS_TOOLS/run-command.sh
echo "Running test ... end"
