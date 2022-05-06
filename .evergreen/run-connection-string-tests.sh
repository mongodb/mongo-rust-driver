#!/bin/bash

set -o errexit
set -o xtrace

. ~/.cargo/env

RUST_BACKTRACE=1 cargo test --features aws-auth spec::auth -- --test-threads 1
RUST_BACKTRACE=1 cargo test --features aws-auth uri_options -- --test-threads 1
RUST_BACKTRACE=1 cargo test --features aws-auth connection_string -- --test-threads 1
