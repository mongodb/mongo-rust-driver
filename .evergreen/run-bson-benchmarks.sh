#!/bin/sh

set -o errexit

cd benchmarks
cargo run \
      --release \
      -- --output="../benchmark-results.json" --bson
