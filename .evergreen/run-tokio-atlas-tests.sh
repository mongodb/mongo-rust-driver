#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh
RUST_BACKTRACE=1 cargo test atlas_connectivity

