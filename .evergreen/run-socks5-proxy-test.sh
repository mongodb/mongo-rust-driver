#!/bin/bash

set -o errexit

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("socks5-proxy")
CARGO_OPTIONS+=("--ignore-default-filter")

if [ "$OPENSSL" = true ]; then
  FEATURE_FLAGS+=("openssl-tls")
fi

# with auth
$PYTHON3 $DRIVERS_TOOLS/.evergreen/socks5srv.py --port 1080 --auth "username:p4ssw0rd" --map "localhost:12345 to localhost:27017" &
AUTH_PID=$!
# without auth
$PYTHON3 $DRIVERS_TOOLS/.evergreen/socks5srv.py --port 1081 --map "localhost:12345 to localhost:27017" &
NOAUTH_PID=$!

set +o errexit

cargo_test socks5_proxy

kill $AUTH_PID
kill $NOAUTH_PID

exit ${CARGO_RESULT}
