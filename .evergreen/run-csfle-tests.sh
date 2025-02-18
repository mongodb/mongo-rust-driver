#!/usr/bin/env bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

export CSFLE_TLS_CERT_DIR="${DRIVERS_TOOLS}/.evergreen/x509gen"

FEATURE_FLAGS+=("in-use-encryption" "azure-kms")

if [[ "$OPENSSL" = true ]]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

if [ "$OS" = "Windows_NT" ]; then
  export CSFLE_TLS_CERT_DIR=$(cygpath ${CSFLE_TLS_CERT_DIR} --windows)
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

. ./secrets-export.sh

# Add mongodb binaries to path for mongocryptd
PATH=${PATH}:${DRIVERS_TOOLS}/mongodb/bin

set +o errexit

cargo_test test::csfle
cargo_test test::spec::client_side_encryption

feature_flags+=("aws-auth")
cargo_test test::csfle::prose::on_demand_aws::success

# Unset variables for on-demand credential failure tests.
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
cargo_test test::csfle::prose::on_demand_aws::failure

exit ${CARGO_RESULT}
