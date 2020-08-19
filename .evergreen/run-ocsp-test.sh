#!/bin/bash

set -o xtrace
set -o errexit  # Exit the script with error if any of the commands fail

############################################
#            Main Program                  #
############################################

# Supported/used environment variables:
# MONGODB_URI                Set the URI, including an optional username/password to use
#                            to connect to the server via 
# ASYNC_RUNTIME              Specify the async runtime to use. Must be either "tokio" or
#                            "async-std".
# OCSP_TLS_SHOULD_SUCCEED    Whether the connection attempt should succeed or not with the
#                            given configuration.
# OCSP_ALGORITHM             Specify the cyptographic algorithm used to sign the server's
#                            certificate. Must be either "rsa" or "ecdsa".

echo "Running OCSP test"

# show test output
set -x

set -o errexit

if [[ "$MONGODB_URI" == *"?"* ]]; then
    export MONGODB_URI="${MONGODB_URI}&"
else
    export MONGODB_URI="${MONGODB_URI}/?"
fi
CA_FILE=`echo "${DRIVERS_TOOLS}/.evergreen/ocsp/${OCSP_ALGORITHM}/ca.pem" | sed 's/\//%2F/g'`

export MONGODB_URI="${MONGODB_URI}tls=true&tlsCAFile=${CA_FILE}"

. ~/.cargo/env

RUST_BACKTRACE=1 cargo test spec::ocsp
