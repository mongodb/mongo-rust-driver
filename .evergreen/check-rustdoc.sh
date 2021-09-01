#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo rustdoc -- -D warnings
cargo rustdoc --no-default-features --features async-std-runtime -- -D warnings
cargo rustdoc --no-default-features --features sync -- -D warnings
