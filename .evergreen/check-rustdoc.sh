#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

# docs.rs builds the driver on a read-only file system. to create a more realistic environment, we first
# build the driver to ensure we have all the deps already in src, and then limit the permissions on that directory.
# this is to help us avoid introducing problems like those described here 
# https://docs.rs/about/builds#read-only-directories where we or a dependency modify source code during the
# build process.
cargo +nightly build
cargo +nightly --no-default-features --features async-std-runtime
cargo +nightly --no-default-features --features sync
cargo +nightly --features tokio-sync
cargo clean

chmod -R 555 ${CARGO_HOME}/registry/src

cargo +nightly rustdoc -- -D warnings --cfg docsrs
cargo +nightly rustdoc --no-default-features --features async-std-runtime -- -D warnings --cfg docsrs
cargo +nightly rustdoc --no-default-features --features sync -- -D warnings --cfg docsrs
cargo +nightly rustdoc --features tokio-sync -- -D warnings --cfg docsrs
