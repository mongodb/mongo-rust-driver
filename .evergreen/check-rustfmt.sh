#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh
rustfmt +nightly --unstable-features --check src/**/*.rs
rustfmt +nightly --unstable-features --check src/*.rs
