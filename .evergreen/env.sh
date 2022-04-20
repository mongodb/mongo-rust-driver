#!/bin/bash

export PATH="/opt/mongodbtoolchain/v2/bin:$PATH"
export RUSTUP_HOME="${PROJECT_DIRECTORY}/.rustup"
export PATH="${RUSTUP_HOME}/bin:$PATH"
export CARGO_HOME="${PROJECT_DIRECTORY}/.cargo"
export PATH="${CARGO_HOME}/bin:$PATH"

if [ "Windows_NT" == "$OS" ]; then
    # rustup/cargo need the native Windows paths; $PROJECT_DIRECTORY is a cygwin path
    export RUSTUP_HOME=$(cygpath ${RUSTUP_HOME} --windows)
    export CARGO_HOME=$(cygpath ${CARGO_HOME} --windows)
fi

source ${CARGO_HOME}/env

NODE_ARTIFACTS_PATH="${PROJECT_DIRECTORY}/node-artifacts"
export NVM_DIR="${NODE_ARTIFACTS_PATH}/nvm"

if [[ "$OS" == "Windows_NT" ]]; then
    NVM_HOME=$(cygpath -w "$NVM_DIR")
    export NVM_HOME
    NVM_SYMLINK=$(cygpath -w "$NODE_ARTIFACTS_PATH/bin")
    export NVM_SYMLINK
    NVM_ARTIFACTS_PATH=$(cygpath -w "$NODE_ARTIFACTS_PATH/bin")
    export NVM_ARTIFACTS_PATH
    export OPENSSL_DIR="C:\\openssl"
    OPENSSL_LIB_PATH=$(cygpath $OPENSSL_DIR/lib)
    ls $OPENSSL_LIB_PATH
    PATH=$(cygpath $NVM_SYMLINK):$(cygpath $NVM_HOME):$PATH
    export PATH
    echo "updated path on windows PATH=$PATH"
else
    [ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh"
fi
