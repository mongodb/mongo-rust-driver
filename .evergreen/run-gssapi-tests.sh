#!/bin/bash

set -o xtrace
set -o errexit # Exit the script with error if any of the commands fail

echo "Running MONGODB-GSSAPI authentication tests"

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

# Source the drivers/atlas_connect secrets, where GSSAPI test values are held
source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/atlas_connect

FEATURE_FLAGS+=("gssapi-auth")

set +o errexit

if [ "Windows_NT" != "$OS" ]; then
  # Create a krb5 config file with relevant
  touch krb5.conf
  echo "[realms]
    $SASL_REALM = {
      kdc = $SASL_HOST
      admin_server = $SASL_HOST
    }

    $SASL_REALM_CROSS = {
      kdc = $SASL_HOST
      admin_server = $SASL_HOST
    }

  [domain_realm]
    .$SASL_DOMAIN = $SASL_REALM
    $SASL_DOMAIN = $SASL_REALM
  " > krb5.conf

  export KRB5_CONFIG=krb5.conf

  # Authenticate the user principal in the KDC before running the e2e test
  echo "Authenticating $PRINCIPAL"
  echo "$SASL_PASS" | kinit -p $PRINCIPAL
  klist
fi

# Run end-to-end auth tests for "$PRINCIPAL" user
TEST_OPTIONS+=("--skip with_service_realm_and_host_options")
cargo_test test::auth::gssapi_skip_local

if [ "Windows_NT" != "$OS" ]; then
  # Unauthenticate
  echo "Unauthenticating $PRINCIPAL"
  kdestroy

  # Authenticate the alternative user principal in the KDC and run other e2e test
  echo "Authenticating $PRINCIPAL_CROSS"
  echo "$SASL_PASS_CROSS" | kinit -p $PRINCIPAL_CROSS
  klist
fi

TEST_OPTIONS=()
cargo_test test::auth::gssapi_skip_local::with_service_realm_and_host_options

if [ "Windows_NT" != "$OS" ]; then
  # Unauthenticate
  echo "Unauthenticating $PRINCIPAL_CROSS"
  kdestroy
fi

# Run remaining tests
cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

exit $CARGO_RESULT
