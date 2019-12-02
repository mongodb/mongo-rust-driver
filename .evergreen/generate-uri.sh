#!/usr/bin/env bash

set -o errexit

if [ "$SSL" != "ssl" ]; then
    return
fi

DRIVERS_TOOLS_X509=`echo "$DRIVERS_TOOLS_X509" | sed 's/\//%2F/g'`
CA_FILE="${DRIVERS_TOOLS_X509}%2Fca.pem"
CERT_FILE="${DRIVERS_TOOLS_X509}%2Fclient.pem"

if [[ "$MONGODB_URI" == *"?"* ]]; then
    export MONGODB_URI="${MONGODB_URI}&"
else
    export MONGODB_URI="${MONGODB_URI}/?"
fi


export MONGODB_URI="${MONGODB_URI}tls=true&tlsCAFile=${CA_FILE}&tlsCertificateKeyFile=${CERT_FILE}&tlsAllowInvalidCertificates=true"

echo "MONGODB_URI: ${MONGODB_URI}"
