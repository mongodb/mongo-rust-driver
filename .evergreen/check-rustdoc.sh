#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo +nightly rustdoc -- -D warnings --cfg docsrs
cargo +nightly rustdoc --no-default-features --features async-std-runtime -- -D warnings --cfg docsrs
cargo +nightly rustdoc --no-default-features --features sync -- -D warnings --cfg docsrs
cargo +nightly rustdoc --features tokio-sync -- -D warnings --cfg docsrs
