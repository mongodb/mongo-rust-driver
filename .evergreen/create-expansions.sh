#!/bin/bash

# Get the current unique version of this checkout.
if [[ "$is_patch" = true ]]; then
  CURRENT_VERSION=$(git describe)-patch-${version_id}
else
  CURRENT_VERSION=latest
fi

DRIVERS_TOOLS="$(pwd)/../drivers-tools"

# Python has cygwin path problems on Windows. Detect prospective mongo-orchestration home
# directory.
if [[ "${OS}" = "Windows_NT" ]]; then
  export DRIVERS_TOOLS=$(cygpath -m $DRIVERS_TOOLS)
fi

MONGO_ORCHESTRATION_HOME="${DRIVERS_TOOLS}/.evergreen/orchestration"
MONGODB_BINARIES="${DRIVERS_TOOLS}/mongodb/bin"
UPLOAD_BUCKET="${project}"
PROJECT_DIRECTORY="$(pwd)"
LIBMONGOCRYPT_SUFFIX_DIR="lib"
# The RHEL path is 'lib64', not 'lib'
if [ "${LIBMONGOCRYPT_OS}" = "rhel-80-64-bit" ]; then
  LIBMONGOCRYPT_SUFFIX_DIR="lib64"
fi
MONGOCRYPT_LIB_DIR="${PROJECT_DIRECTORY}/libmongocrypt/${LIBMONGOCRYPT_OS}/${LIBMONGOCRYPT_SUFFIX_DIR}"
LD_LIBRARY_PATH="${MONGOCRYPT_LIB_DIR}:${LD_LIBRARY_PATH}"

TMPDIR="${MONGO_ORCHESTRATION_HOME}/db"
PATH="${MONGODB_BINARIES}:${PATH}"
PROJECT="${project}"

cat <<EOT >expansion.yml
CURRENT_VERSION: "${CURRENT_VERSION}"
DRIVERS_TOOLS: "${DRIVERS_TOOLS}"
MONGO_ORCHESTRATION_HOME: "${MONGO_ORCHESTRATION_HOME}"
MONGODB_BINARIES: "${MONGODB_BINARIES}"
UPLOAD_BUCKET: "${UPLOAD_BUCKET}"
PROJECT_DIRECTORY: "${PROJECT_DIRECTORY}"
MONGOCRYPT_LIB_DIR: "${MONGOCRYPT_LIB_DIR}"
LD_LIBRARY_PATH: "${LD_LIBRARY_PATH}"
TMPDIR: "${TMPDIR}"
PATH: "${PATH}"
PROJECT: "${PROJECT}"
AZURE_IMDS_MOCK_PORT: 8080
PREPARE_SHELL: |
    set -o errexit
    set -o xtrace

    export DRIVERS_TOOLS="${DRIVERS_TOOLS}"
    export MONGO_ORCHESTRATION_HOME="${MONGO_ORCHESTRATION_HOME}"
    export MONGODB_BINARIES="${MONGODB_BINARIES}"
    export UPLOAD_BUCKET="${UPLOAD_BUCKET}"
    export PROJECT_DIRECTORY="${PROJECT_DIRECTORY}"
    export MONGOCRYPT_LIB_DIR="${MONGOCRYPT_LIB_DIR}"
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}"

    export TMPDIR="${TMPDIR}"
    export PATH="${PATH}"
    export PROJECT="${PROJECT}"

    if [[ "$OS" != "Windows_NT" ]]; then
        ulimit -n 64000
    fi
EOT

# Print the expansion file created.
cat expansion.yml
