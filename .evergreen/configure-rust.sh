#!/bin/bash

export RUSTUP_HOME="${PROJECT_DIRECTORY}/.rustup"
export PATH="${RUSTUP_HOME}/bin:$PATH"
export CARGO_HOME="${PROJECT_DIRECTORY}/.cargo"
export PATH="${CARGO_HOME}/bin:$PATH"

if [[ "Windows_NT" == "$OS" ]]; then
    # rustup/cargo need the native Windows paths; $PROJECT_DIRECTORY is a cygwin path
    export RUSTUP_HOME=$(cygpath ${RUSTUP_HOME} --windows)
    export CARGO_HOME=$(cygpath ${CARGO_HOME} --windows)
fi

. ${CARGO_HOME}/env
