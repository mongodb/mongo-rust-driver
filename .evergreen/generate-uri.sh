#!/usr/bin/env bash

set -o errexit

if [ "$SSL" != "ssl" ]; then
    return
fi

DRIVERS_TOOLS_X509=`echo "$DRIVERS_TOOLS_X509" | sed 's/\//%2F/g'`
CA_FILE="${DRIVERS_TOOLS_X509}%2Fca.pem"
CERT_FILE="${DRIVERS_TOOLS_X509}%2Fclient.pem"

update_uri() {
    local ORIG_URI=$1
    if [[ "$ORIG_URI" == "" ]]; then
        return
    fi
    if [[ "$ORIG_URI" == *"?"* ]]; then
        ORIG_URI="${ORIG_URI}&"
    else
        ORIG_URI="${ORIG_URI}/?"
    fi
    echo "${ORIG_URI}tls=true&tlsCAFile=${CA_FILE}&tlsCertificateKeyFile=${CERT_FILE}&tlsAllowInvalidCertificates=true"
}

export MONGODB_URI="$(update_uri ${MONGODB_URI})"
export SINGLE_MONGOS_LB_URI="$(update_uri ${SINGLE_MONGOS_LB_URI})"
export MULTI_MONGOS_LB_URI="$(update_uri ${MULTI_MONGOS_LB_URI})"

echo "MONGODB_URI: ${MONGODB_URI}"
echo "SINGLE_MONGOS_LB_URI: ${SINGLE_MONGOS_LB_URI}"
echo "MULTI_MONGOS_LB_URI: ${MULTI_MONGOS_LB_URI}"