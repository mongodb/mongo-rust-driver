#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh

cargo install --locked cargo-deny
cargo deny --all-features --exclude-unpublished check
