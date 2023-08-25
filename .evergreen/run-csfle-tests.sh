#!/usr/bin/env bash

set -o errexit
set -o pipefail

source ./.evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

export CSFLE_TLS_CERT_DIR="${DRIVERS_TOOLS}/.evergreen/x509gen"

cd ${DRIVERS_TOOLS}/.evergreen/auth_aws
. ./activate-authawsvenv.sh
cd -

FEATURE_FLAGS+=("in-use-encryption-unstable" "aws-auth" "azure-kms")

if [[ "$OPENSSL" = true ]]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

if [ "$OS" = "Windows_NT" ]; then
  export CSFLE_TLS_CERT_DIR=$(cygpath ${CSFLE_TLS_CERT_DIR} --windows)
  export SSL_CERT_FILE=$(cygpath /etc/ssl/certs/ca-bundle.crt --windows)
  export SSL_CERT_DIR=$(cygpath /etc/ssl/certs --windows)
fi

export AWS_DEFAULT_REGION=us-east-1
. ${DRIVERS_TOOLS}/.evergreen/csfle/set-temp-creds.sh

echo "cargo test options: $(cargo_test_options)"

set +o errexit

cargo_test test::csfle prose.xml
cargo_test test::spec::client_side_encryption spec.xml
cargo_test sessions_not_supported sessions.xml

# Unset variables for on-demand credential failure tests.
unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
cargo_test test::csfle::on_demand_aws_failure failure.xml

junit-report-merger results.xml prose.xml spec.xml sessions.xml failure.xml

exit ${CARGO_RESULT}
