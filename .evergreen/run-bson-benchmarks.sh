#!/bin/sh

set -o errexit

pushd benchmarks

cargo run \
      --release \
      -- --output="../benchmark-results.json" --bson

popd
