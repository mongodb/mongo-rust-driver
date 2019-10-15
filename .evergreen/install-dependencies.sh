#!/bin/sh

rm -rf ~/.rustup
curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

. ~/.cargo/env
rustup toolchain install nightly -c rustfmt
