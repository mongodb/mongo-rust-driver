#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo test -- --test-threads=4
