#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly-2019-07-22 fmt -- --check

cd benchmarks
cargo +nightly-2019-07-22 fmt -- --check
