#!/bin/sh

set -o errexit

. ~/.cargo/env
RUST_BACKTRACE=1 cargo test atlas_connectivity --no-default-features --features async-std-runtime

