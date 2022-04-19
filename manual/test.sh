#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

cd $(dirname $0)/deps
cargo build
cd ..
mdbook test -L deps/target/debug/deps