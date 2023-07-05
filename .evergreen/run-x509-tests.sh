#!/bin/bash

set -o errexit
set -o xtrace 
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

export SUBJECT=`openssl x509 -subject -nameopt RFC2253 -noout -inform PEM -in $CERT_PATH`

# Strip `subject=` prefix from the subject
export SUBJECT=${SUBJECT#"subject="}

# Remove any leading or trailing whitespace
export SUBJECT=`echo "$SUBJECT" | awk '{$1=$1;print}'`

use_async_runtime

FEATURE_FLAGS+=("${TLS_FEATURE}")

set +o errexit

MONGO_X509_USER="$SUBJECT" cargo_test > results.xml

exit ${CARGO_RESULT}
