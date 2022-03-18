#!/bin/bash

set -o errexit
set -o xtrace 
set -o pipefail

source ./.evergreen/env.sh

export SUBJECT=`openssl x509 -subject -nameopt RFC2253 -noout -inform PEM -in $CERT_PATH`

# Strip `subject=` prefix from the subject
export SUBJECT=${SUBJECT#"subject="}

# Remove any leading or trailing whitespace
export SUBJECT=`echo "$SUBJECT" | awk '{$1=$1;print}'`

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    RUNTIME_FEATURE="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    RUNTIME_FEATURE="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

OPTIONS="--no-default-features --features ${RUNTIME_FEATURE},${TLS_FEATURE} -- -Z unstable-options --format json --report-time"

set +o errexit
RUST_BACKTRACE=1 MONGO_X509_USER="$SUBJECT" cargo test x509 $OPTIONS | tee results.json
CARGO_EXIT=$?
cat results.json | cargo2junit > results.xml
exit $CARGO_EXIT
