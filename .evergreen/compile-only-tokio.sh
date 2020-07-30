#!/bin/sh

set -o errexit

. ~/.cargo/env

rustup run $RUST_VERSION cargo build
