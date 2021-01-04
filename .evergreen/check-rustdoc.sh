#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo doc
cargo doc --no-default-features --features async-std-runtime
cargo doc --no-default-features --features sync
