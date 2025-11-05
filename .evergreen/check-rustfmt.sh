#!/bin/bash

set -o errexit

source ./.evergreen/env.sh
rustfmt +nightly --unstable-features --check driver/src/**/*.rs
rustfmt +nightly --unstable-features --check driver/src/*.rs
