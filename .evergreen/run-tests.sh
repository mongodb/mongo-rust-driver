#!/bin/sh

set -o errexit

. ~/.cargo/env
RUST_BACKTRACE=1 CARGO_NET_GIT_FETCH_WITH_CLI=true cargo test
