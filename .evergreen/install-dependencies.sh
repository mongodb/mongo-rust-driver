#!/bin/sh

rm -rf ~/.rustup
curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

. ~/.cargo/env
rustup component add clippy
rustup update nightly
rustup component add rustfmt --toolchain nightly
