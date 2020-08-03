#!/bin/sh

# Make sure to use msvc toolchain rather than gnu, which is the default for cygwin
if [ "Windows_NT" == "$OS" ]; then
    export DEFAULT_HOST_OPTIONS='--default-host x86_64-pc-windows-msvc'
fi

rm -rf ~/.rustup
curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path $DEFAULT_HOST_OPTIONS

# rustup installs into C:\Users\$USER instead of C:\home\$USER, so we symlink both .rustup and .cargo
if [ "Windows_NT" == "$OS" ]; then
    ln -s /cygdrive/c/Users/$USER/.rustup/ ~/.rustup
    ln -s /cygdrive/c/Users/$USER/.cargo/ ~/.cargo
fi

# This file is not created by default on Windows
echo 'export PATH=$PATH:~/.cargo/bin' >> ~/.cargo/env

echo "export CARGO_NET_GIT_FETCH_WITH_CLI=true" >> ~/.cargo/env

. ~/.cargo/env

# Install nightly rustfmt
rustup toolchain install nightly -c rustfmt
