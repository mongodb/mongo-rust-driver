#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

FEATURES=""

if [ "$ASYNC_RUNTIME" = "tokio" ]; then
    FEATURES="tokio-runtime"
elif [ "$ASYNC_RUNTIME" = "async-std" ]; then
    FEATURES="async-std-runtime"
else
    echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
    exit 1
fi

rustc --version

SECONDS=0
cargo build --release
DURATION_IN_SECONDS="$SECONDS"

cat > benchmark-results.json <<-EOF
[
  {
    "info": {
      "test_name": "Compile Time",
      "args": {}
    },
    "metrics": [
      {
        "name": "compile_time_seconds",
        "value": $DURATION_IN_SECONDS
      }
    ]
  }
]
EOF

cat benchmark-results.json
