#!/bin/sh

set -o errexit

. ~/.cargo/env

cd benchmarks
cargo run \
      --release \
      -- --output="../benchmark-results.json" --bson

cat ../benchmark-results.json
