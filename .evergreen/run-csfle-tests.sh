#!/usr/bin/env bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

export CSFLE_TLS_CERT_DIR="${DRIVERS_TOOLS}/.evergreen/x509gen"

FEATURE_FLAGS+=("in-use-encryption" "aws-auth" "azure-kms")

if [[ "$OPENSSL" = true ]]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

if [ "$OS" = "Windows_NT" ]; then
  export CSFLE_TLS_CERT_DIR=$(cygpath ${CSFLE_TLS_CERT_DIR} --windows)
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

. ./secrets-export.sh

set +o errexit

cargo_test test::csfle prose.xml
cargo_test test::spec::client_side_encryption spec.xml

# Unset variables for on-demand credential failure tests.
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
cargo_test test::csfle::on_demand_aws_failure failure.xml

merge-junit -o results.xml prose.xml spec.xml failure.xml

exit ${CARGO_RESULT}
