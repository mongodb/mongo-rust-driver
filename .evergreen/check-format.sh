#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly fmt -- --check
