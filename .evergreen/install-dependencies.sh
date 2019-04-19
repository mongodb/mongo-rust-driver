#!/bin/sh

curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

ls -al ~/.cargo
. ~/.cargo/env
rustup update nightly
rustup component add rustfmt --toolchain nightly
