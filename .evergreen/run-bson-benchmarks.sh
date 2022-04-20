#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

cd benchmarks
cargo run \
      --release \
      -- --output="../benchmark-results.json" --bson

cat ../benchmark-results.json
