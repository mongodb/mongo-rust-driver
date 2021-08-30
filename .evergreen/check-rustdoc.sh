#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly rustdoc -p bson --all-features -- --cfg docsrs -D warnings
