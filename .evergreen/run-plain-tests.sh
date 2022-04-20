#!/bin/bash

set -o errexit
set -o xtrace 

source ./.evergreen/configure-rust.sh

RUST_BACKTRACE=1 MONGO_PLAIN_AUTH_TEST=1 cargo test plain
