#!/bin/bash

CARGO_OPTIONS=()
TEST_OPTIONS=()
CARGO_RESULT=0

. .evergreen/features.sh

if [[ -f nextest-archive.tar.zst ]]; then
  WORKSPACE_ROOT="$(pwd)"
  if [[ "Windows_NT" = "$OS" ]]; then
    WORKSPACE_ROOT="$(cygpath -w ${WORKSPACE_ROOT})"
  fi
  CARGO_OPTIONS+=("--archive-file" "nextest-archive.tar.zst" "--workspace-remap" "${WORKSPACE_ROOT}")
fi


cargo_test_options() {
  FEATURES="$(features_option)"
  if [[ -f nextest-archive.tar.zst ]]; then
    # Feature flags are set when the archive is built
    FEATURES=""
  fi
  echo $1 ${CARGO_OPTIONS[@]} "${FEATURES}" -- ${TEST_OPTIONS[@]}
}

cargo_test() {
  LOG_PATH=$(mktemp)
  tail -f ${LOG_PATH} &
  TAIL_PID=$!
  LOG_UNCAPTURED=${LOG_PATH} RUST_BACKTRACE=1 cargo nextest run --profile ci $(cargo_test_options $1)
  ((CARGO_RESULT = ${CARGO_RESULT} || $?))
  if [[ -f "results.xml" ]]; then
    mv results.xml previous.xml
    merge-junit -o results.xml previous.xml target/nextest/ci/junit.xml
  else
    mv target/nextest/ci/junit.xml results.xml
  fi
  kill ${TAIL_PID}
  rm ${LOG_PATH}
}
