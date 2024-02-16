#!/bin/bash

set -o errexit

if [[ "$SSL" = "ssl" ]]; then
  DRIVERS_TOOLS_X509="${DRIVERS_TOOLS}/.evergreen/x509gen"
  DRIVERS_TOOLS_X509_ENCODED=$(echo "$DRIVERS_TOOLS_X509" | sed 's/\//%2F/g')
  CA_FILE="${DRIVERS_TOOLS_X509_ENCODED}%2Fca.pem"
  CERT_FILE="${DRIVERS_TOOLS_X509_ENCODED}%2Fclient.pem"

  update_uri() {
    local ORIG_URI=$1
    if [[ "$ORIG_URI" == "" ]]; then
      return
    fi
    # The rustls library requires a domain name.
    ORIG_URI=$(echo "$ORIG_URI" | sed s/127.0.0.1/localhost/)
    if [[ "$ORIG_URI" == *"?"* ]]; then
      ORIG_URI="${ORIG_URI}&"
    else
      ORIG_URI="${ORIG_URI}/?"
    fi
    if [[ "$ORIG_URI" != *"tls=true"* ]]; then
      ORIG_URI="${ORIG_URI}tls=true&"
    fi
    echo "${ORIG_URI}tlsCAFile=${CA_FILE}&tlsCertificateKeyFile=${CERT_FILE}&tlsAllowInvalidCertificates=true"
  }

  MONGODB_URI="$(update_uri ${MONGODB_URI})"
  SINGLE_MONGOS_LB_URI="$(update_uri ${SINGLE_MONGOS_LB_URI})"
  MULTI_MONGOS_LB_URI="$(update_uri ${MULTI_MONGOS_LB_URI})"
fi

cat <<EOT >uri-expansions.yml
MONGODB_URI: ${MONGODB_URI}
SINGLE_MONGOS_LB_URI: ${SINGLE_MONGOS_LB_URI}
MULTI_MONGOS_LB_URI: ${MULTI_MONGOS_LB_URI}
EOT

# Print generated URIs
cat uri-expansions.yml
