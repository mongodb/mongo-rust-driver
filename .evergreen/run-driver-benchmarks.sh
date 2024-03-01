#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

cd benchmarks
cargo run \
  --release \
  -- --output="../benchmark-results.json" --driver

cat ../benchmark-results.json
