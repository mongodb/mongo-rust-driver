#!/bin/sh

set -o errexit

. ~/.cargo/env

cd $(dirname $0)/deps
cargo build
cd ..
mdbook test -L deps/target/debug/deps