#!/bin/bash

set -o errexit

. ~/.cargo/env

FEATURES=""

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURES="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURES="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

RUST_VERSION=$(rustc --version)
SECONDS=0
cargo build --release
DURATION_IN_SECONDS="$SECONDS"

cat > benchmark-results.json <<-EOF
[
  {
    "info": {
      "test_name": "Compile Time",
      "args": {
        "Rust Version": "$RUST_VERSION"
      }
    },
    "metrics": [
      {
        "name": "Compile Time (s)",
        "value": $DURATION_IN_SECONDS
      }
    ]
  }
]
EOF

cat benchmark-results.json
