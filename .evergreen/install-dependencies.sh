#!/bin/sh

rm -rf ~/.rustup
curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path

echo "export CARGO_NET_GIT_FETCH_WITH_CLI=true" >> ~/.cargo/env
. ~/.cargo/env
rustup toolchain install nightly -c rustfmt
