#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo clippy --all-targets --all-features -p mongodb -- -D warnings
