#!/bin/bash

export RUSTUP_HOME="${PROJECT_DIRECTORY}/.rustup"
export PATH="${RUSTUP_HOME}/bin:$PATH"
export CARGO_HOME="${PROJECT_DIRECTORY}/.cargo"
export PATH="${CARGO_HOME}/bin:$PATH"
NODE_ARTIFACTS_PATH="${PROJECT_DIRECTORY}/node-artifacts"
export NVM_DIR="${NODE_ARTIFACTS_PATH}/nvm"

. ${CARGO_HOME}/env

if [[ "$OSTYPE" == "cygwin" ]]; then
  # Update path for DLLs
  export PATH="${MONGOCRYPT_LIB_DIR}/../bin:$PATH"

  # rustup/cargo need the native Windows paths; $PROJECT_DIRECTORY is a cygwin path
  export RUSTUP_HOME=$(cygpath ${RUSTUP_HOME} --windows)
  export CARGO_HOME=$(cygpath ${CARGO_HOME} --windows)
  export MONGOCRYPT_LIB_DIR=$(cygpath ${MONGOCRYPT_LIB_DIR} --windows)

  NVM_HOME=$(cygpath -w "$NVM_DIR")
  export NVM_HOME
  NVM_SYMLINK=$(cygpath -w "$NODE_ARTIFACTS_PATH/bin")
  export NVM_SYMLINK
  NVM_ARTIFACTS_PATH=$(cygpath -w "$NODE_ARTIFACTS_PATH/bin")
  export NVM_ARTIFACTS_PATH
  PATH=$(cygpath $NVM_SYMLINK):$(cygpath $NVM_HOME):$PATH
  export PATH
  echo "updated path on windows PATH=$PATH"

  export OPENSSL_INCLUDE_DIR="C:\\Program Files\\OpenSSL-Win64\\include"
  export OPENSSL_LIB_DIR="C:\\Program Files\\OpenSSL-Win64\\lib\\VC\\x64\\MD"
else
  # Turn off tracing for the very-spammy nvm script.
  set +o xtrace
  [ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh"
  set -o xtrace
fi
