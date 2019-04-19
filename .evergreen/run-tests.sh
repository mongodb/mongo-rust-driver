#!/bin/sh

set -o errexit

. ~/.cargo/env
cargo test
