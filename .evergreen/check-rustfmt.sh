#!/bin/bash

set -o errexit

source ./.evergreen/env.sh
cargo +nightly fmt --check -- --unstable-features
