#!/bin/bash

# Tests building the driver when the bson and mongocrypt dependencies are downloaded from crates.io
# rather than git.

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

cargo new test-downloaded-dependency
cd test-downloaded-dependency
cargo add mongodb --features in-use-encryption-unstable
cargo build
