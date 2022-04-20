#!/bin/sh

set -o errexit

source ./.evergreen/env.sh

cd benchmarks
cargo run \
      --release \
      -- --output="../benchmark-results.json" --bson

cat ../benchmark-results.json
