#!/bin/sh

set -o errexit

source ./.evergreen/env.sh
RUST_BACKTRACE=1 cargo test atlas_connectivity --no-default-features --features async-std-runtime

