#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

FEATURES=""

cd benchmarks
cargo run \
  --release \
  --no-default-features \
  --features ${FEATURES} \
  -- --output="../benchmark-results.json" --driver

cat ../benchmark-results.json
