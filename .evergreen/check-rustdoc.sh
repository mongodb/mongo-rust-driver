#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

# docs.rs builds the driver on a read-only file system. to create a more realistic environment, we first
# build the driver to ensure we have all the deps already in src, and then limit the permissions on that directory
# and rebuild the docs.
# this is to help us avoid introducing problems like those described here
# https://docs.rs/about/builds#read-only-directories where we or a dependency modify source code during the
# build process.

# build with all available features to ensure all optional dependencies are brought in too.
cargo +nightly build --all-features
cargo clean

chmod -R 555 ${CARGO_HOME}/registry/src

# this invocation mirrors the way docs.rs builds our documentation (see the [package.metadata.docs.rs] section
# in Cargo.toml).
cargo +nightly rustdoc --all-features -- -D warnings --cfg docsrs
