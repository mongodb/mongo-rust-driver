#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly rustdoc --features aws-auth -- -D warnings
cargo +nightly rustdoc --no-default-features --features async-std-runtime -- -D warnings
cargo +nightly rustdoc --no-default-features --features sync -- -D warnings
