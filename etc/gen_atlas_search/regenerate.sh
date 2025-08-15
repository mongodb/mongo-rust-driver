#!/usr/bin/env bash

set -o errexit
set -x

gen_path="$(dirname $0)/../../src/atlas_search/gen.rs"

cargo run > ${gen_path}
rustfmt +nightly --unstable-features ${gen_path}