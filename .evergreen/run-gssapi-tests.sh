#!/bin/bash

set -o xtrace
set -o errexit

echo "Running MONGODB-GSSAPI authentication tests"

cd "$PROJECT_DIRECTORY"
source .evergreen/env.sh
source .evergreen/cargo-test.sh

source "$DRIVERS_TOOLS/.evergreen/secrets_handling/setup-secrets.sh" drivers/enterprise_auth

# Windows authenticates via SSPI with a username/password pair in the URI.
if [ "$OSTYPE" != "cygwin" ]; then
  export KRB5_CONFIG="krb5.conf.empty" # a nonexistent file is equivalent to an empty config
  KEYTAB_FILE="drivers.keytab"

  set +o xtrace
  echo "$KEYTAB_BASE64" | base64 --decode > "$KEYTAB_FILE"
  set -o xtrace

  kdestroy -A
  kinit -k -t "$KEYTAB_FILE" -p "$PRINCIPAL"
fi

set +o errexit

FEATURE_FLAGS+=("gssapi-auth")
cargo_test gssapi
cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

if [ "$OSTYPE" != "cygwin" ]; then
  rm "$KEYTAB_FILE"
  kdestroy -A
fi

exit $CARGO_RESULT
