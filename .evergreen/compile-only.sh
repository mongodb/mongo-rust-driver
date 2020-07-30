#!/bin/sh

set -o errexit

. ~/.cargo/env
rustup update $RUST_VERSION

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    .evergreen/compile-only-tokio.sh 
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    .evergreen/compile-only-async-std.sh
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi
