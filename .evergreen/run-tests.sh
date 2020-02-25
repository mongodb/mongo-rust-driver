#!/bin/sh

set -o errexit

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    .evergreen/run-tokio-tests.sh
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    .evergreen/run-async-std-tests.sh
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi
