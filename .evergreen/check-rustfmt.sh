#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly fmt -- --check

cd benchmarks
cargo +nightly fmt -- --check


