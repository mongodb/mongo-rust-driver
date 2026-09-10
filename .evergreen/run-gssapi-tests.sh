#!/bin/bash

set -o xtrace
set -o errexit

echo "Running MONGODB-GSSAPI authentication tests"

cd ${PROJECT_DIRECTORY}
source .evergreen/env.sh
source .evergreen/cargo-test.sh

source "${DRIVERS_TOOLS}/.evergreen/find-python3.sh"
PYTHON3=$(find_python3)

source "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" drivers/enterprise_auth

export KRB5_CONFIG="krb5.conf.empty" # a nonexistent file is equivalent to an empty config
KEYTAB_FILE="drivers.keytab"

set +o xtrace
python3 -c 'import base64,os,sys; sys.stdout.buffer.write(base64.b64decode(os.environ["KEYTAB_BASE64"]))' > "${KEYTAB_FILE}"
set -o xtrace

kdestroy -A
kinit -k -t $KEYTAB_FILE -p ${PRINCIPAL}

set +o errexit

FEATURE_FLAGS+=("gssapi-auth")
cargo_test gssapi
cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

rm $KEYTAB_FILE
kdestroy -A
exit $CARGO_RESULT
