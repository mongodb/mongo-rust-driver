#!/bin/bash

set -o errexit

source ./.evergreen/env.sh
cargo +${RUSTFMT_VERSION} fmt --check -- --unstable-features
