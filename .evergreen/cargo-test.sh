#!/bin/bash

CARGO_OPTIONS=()
TEST_OPTIONS=()
CARGO_RESULT=0

. .evergreen/features.sh

cargo_test_options() {
  echo $1 ${CARGO_OPTIONS[@]} ${features_option} -- ${TEST_OPTIONS[@]}
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
    mv target/nextest/ci/junit.xml results.xml || true
  fi
  kill ${TAIL_PID}
  rm ${LOG_PATH}
}
