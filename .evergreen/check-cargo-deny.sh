#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

cargo install --locked cargo-deny
cargo deny --all-features check