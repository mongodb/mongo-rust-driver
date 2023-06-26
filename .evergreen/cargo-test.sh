#!/bin/bash

CARGO_OPTIONS=()
TEST_OPTIONS=("-Z unstable-options" "--format json" "--report-time")
FEATURE_FLAGS=()
CARGO_RESULT=0

use_async_runtime() {
    if [ "${ASYNC_RUNTIME}" = "async-std" ]; then
        FEATURE_FLAGS+=("async-std-runtime")
        CARGO_OPTIONS+=("--no-default-features")
    elif [ "${ASYNC_RUNTIME}" != "tokio" ]; then
        echo "invalid async runtime: ${ASYNC_RUNTIME}" >&2
        exit 1
    fi
}

join_by() { local IFS="$1"; shift; echo "$*"; }

cargo_test_options() {
    local FILTERED=()
    for FEAT in "${FEATURE_FLAGS[@]}"; do
        [[ "${FEAT}" != "" ]] && FILTERED+=("${FEAT}")
    done
    local FEATURE_OPTION=""
    if (( ${#FILTERED[@]} != 0 )); then
        FEATURE_OPTION="--features $(join_by , "${FILTERED[@]}")"
    fi
    local THREAD_OPTION=""
    if [ "${SINGLE_THREAD}" = true ]; then
        THREAD_OPTION="--test-threads=1"
    fi
    echo $1 ${CARGO_OPTIONS[@]} ${FEATURE_OPTION} -- ${TEST_OPTIONS[@]} ${THREAD_OPTION}
}

cargo_test() {
    RUST_BACKTRACE=1 \
        cargo test $(cargo_test_options $1) \
        | grep -v '{"t":' \
        | cargo2junit
    (( CARGO_RESULT = ${CARGO_RESULT} || $? ))
}
