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

  # Probe for the openssl install path rather than hardcoding one that keeps changing.
  OPENSSL_ROOT="${OPENSSL_ROOT:-/cygdrive/c/Program Files/OpenSSL-Win64}"
  if [ ! -d "${OPENSSL_ROOT}" ]; then
    echo "OpenSSL install not found at ${OPENSSL_ROOT}" >&2
    exit 1
  fi
  openssl_lib_dir=""
  while IFS= read -r candidate; do
    case "${candidate}" in
      *static*|*MT*|*MTd*|*MDd*) continue ;;
    esac
    [ -f "${candidate}/libcrypto.lib" ] || continue
    # Prefer an explicit MD directory if one exists (matching the Rust toolschain)
    if [ -z "${openssl_lib_dir}" ] || [ "${candidate}" != "${candidate%MD}" ]; then
      openssl_lib_dir="${candidate}"
    fi
  done < <(find "${OPENSSL_ROOT}" -name 'libssl.lib' -printf '%h\n' | sort)
  openssl_header=$(find "${OPENSSL_ROOT}" -path '*/openssl/ssl.h' | head -n 1)
  openssl_include_dir=$(dirname "$(dirname "${openssl_header}")")
  if [ -z "${openssl_lib_dir}" ] || [ -z "${openssl_header}" ]; then
    echo "Could not locate OpenSSL libraries/headers under ${OPENSSL_ROOT}:" >&2
    find "${OPENSSL_ROOT}" \( -name 'libssl.lib' -o -name 'ssl.h' \) >&2
    exit 1
  fi
  OPENSSL_LIB_DIR=$(cygpath --windows "${openssl_lib_dir}")
  export OPENSSL_LIB_DIR
  OPENSSL_INCLUDE_DIR=$(cygpath --windows "${openssl_include_dir}")
  export OPENSSL_INCLUDE_DIR
  echo "OPENSSL_LIB_DIR=${OPENSSL_LIB_DIR} OPENSSL_INCLUDE_DIR=${OPENSSL_INCLUDE_DIR}"
else
  # Turn off tracing for the very-spammy nvm script.
  set +o xtrace
  [ -s "$NVM_DIR/nvm.sh" ] && source "$NVM_DIR/nvm.sh"
  set -o xtrace
fi
