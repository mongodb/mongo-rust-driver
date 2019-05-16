#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo clippy --tests --all-targets --all-features -- -D warnings
