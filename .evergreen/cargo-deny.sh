#!/bin/bash

set -o errexit

. ~/.cargo/env

cargo install --locked cargo-deny
cargo deny check