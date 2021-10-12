#!/bin/bash

set -o errexit

. ~/.cargo/env
rustfmt +nightly --unstable-features --check src/**/*.rs
rustfmt +nightly --unstable-features --check src/*.rs
