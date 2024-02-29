#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

FEATURES=""

rustc --version

SECONDS=0
cargo build --release
DURATION_IN_SECONDS="$SECONDS"

cat >benchmark-results.json <<-EOF
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
