#!/bin/bash

set -o errexit
set -o xtrace 

. ~/.cargo/env

export SUBJECT=`openssl x509 -subject -nameopt RFC2253 -noout -inform PEM -in $CERT_PATH`

# Strip `subject=` prefix from the subject
export SUBJECT=${SUBJECT#"subject="}

# Remove any leading or trailing whitespace
export SUBJECT=`echo "$SUBJECT" | awk '{$1=$1;print}'`

RUST_BACKTRACE=1 MONGO_X509_USER="$SUBJECT" cargo test x509
