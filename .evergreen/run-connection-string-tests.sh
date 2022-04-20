#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

RUST_BACKTRACE=1 cargo test --features aws-auth spec::auth
RUST_BACKTRACE=1 cargo test --features aws-auth uri_options
RUST_BACKTRACE=1 cargo test --features aws-auth connection_string
