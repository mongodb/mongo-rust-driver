#!/bin/bash

# this script accepts one or more of the following arguments: `rust`, `mdbook`, `junit-dependencies`.
# the corresponding dependencies will be installed in the order they are specified.

set -o xtrace
set -o errexit

export RUSTUP_HOME="${PROJECT_DIRECTORY}/.rustup"
export CARGO_HOME="${PROJECT_DIRECTORY}/.cargo"

# Make sure to use msvc toolchain rather than gnu, which is the default for cygwin
if [ "Windows_NT" == "$OS" ]; then
  export DEFAULT_HOST_OPTIONS='--default-host x86_64-pc-windows-msvc'
  # rustup/cargo need the native Windows paths; $PROJECT_DIRECTORY is a cygwin path
  export RUSTUP_HOME=$(cygpath ${RUSTUP_HOME} --windows)
  export CARGO_HOME=$(cygpath ${CARGO_HOME} --windows)
fi

for arg; do
  if [ $arg == "rust" ]; then
    curl https://sh.rustup.rs -sSf | sh -s -- -y --no-modify-path $DEFAULT_HOST_OPTIONS

    # This file is not created by default on Windows
    echo 'export PATH="$PATH:${CARGO_HOME}/bin"' >>${CARGO_HOME}/env
    echo "export CARGO_NET_GIT_FETCH_WITH_CLI=true" >>${CARGO_HOME}/env

    source .evergreen/env.sh
    rustup toolchain install nightly -c rustfmt
  elif [ $arg == "junit-dependencies" ]; then
    source ${CARGO_HOME}/env

    source ./.evergreen/env.sh

    # Install junit-compatible test runner
    cargo install cargo-nextest --locked

    # Install tool for merging different junit reports into a single one
    cargo install merge-junit
  elif [ $arg == "libmongocrypt" ]; then
    mkdir ${PROJECT_DIRECTORY}/libmongocrypt
    cd ${PROJECT_DIRECTORY}/libmongocrypt
    #curl -sSfO https://s3.amazonaws.com/mciuploads/libmongocrypt/all/master/latest/libmongocrypt-all.tar.gz
    curl -sSfO https://mciuploads.s3.amazonaws.com/libmongocrypt/all/1.11.0/libmongocrypt-all.tar.gz
    tar xzf libmongocrypt-all.tar.gz
    if [ "Windows_NT" == "$OS" ]; then
      chmod +x ${MONGOCRYPT_LIB_DIR}/../bin/*.dll
    fi
  elif [ $arg == "cargo-lambda" ]; then
    source ${CARGO_HOME}/env
    cargo install cargo-binstall
    cargo binstall cargo-lambda -y
  else
    echo Missing/unknown install option: "$arg"
    exit 1
  fi
done
