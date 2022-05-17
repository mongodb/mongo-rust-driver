#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

# docs.rs builds the driver on a read-only file system. to create a more realistic environment, we first
# build the driver to ensure we have all the deps already in src, and then limit the permissions on that directory
# and rebuild the docs.
# this is to help us avoid introducing problems like those described here 
# https://docs.rs/about/builds#read-only-directories where we or a dependency modify source code during the
# build process.

source ./.evergreen/feature-combinations.sh

# build with all available features to ensure all optional dependencies are brought in too.
for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
    cargo +nightly build ${FEATURE_COMBINATIONS[$i]}
done
cargo clean

chmod -R 555 ${CARGO_HOME}/registry/src

for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
    cargo +nightly rustdoc ${FEATURE_COMBINATIONS[$i]} -- -D warnings --cfg docsrs
done

