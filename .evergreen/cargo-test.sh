#!/bin/bash

CARGO_OPTIONS=()
TEST_OPTIONS=()
FEATURE_FLAGS=()
CARGO_RESULT=0

join_by() {
  local IFS="$1"
  shift
  echo "$*"
}

cargo_test_options() {
  local FILTERED=()
  for FEAT in "${FEATURE_FLAGS[@]}"; do
    [[ "${FEAT}" != "" ]] && FILTERED+=("${FEAT}")
  done
  local FEATURE_OPTION=""
  if ((${#FILTERED[@]} != 0)); then
    FEATURE_OPTION="--features $(join_by , "${FILTERED[@]}")"
  fi
  echo $1 ${CARGO_OPTIONS[@]} ${FEATURE_OPTION} -- ${TEST_OPTIONS[@]}
}

cargo_test() {
  RUST_BACKTRACE=1 cargo nextest run --profile ci $(cargo_test_options $1)
  ((CARGO_RESULT = ${CARGO_RESULT} || $?))
  mv target/nextest/ci/junit.xml $2
}
