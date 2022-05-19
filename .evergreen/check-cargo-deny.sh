#!/bin/bash

set -o errexit

source ./.evergreen/configure-rust.sh

cargo install --locked cargo-deny
cargo deny --all-features check