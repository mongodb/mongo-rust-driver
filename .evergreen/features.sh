#!/bin/bash

FEATURE_FLAGS=()

STANDARD_FEATURES=("tracing-unstable" "cert-key-password" "opentelemetry" "error-backtrace")

_join_by() {
    local IFS="$1"
    shift
    echo "$*"
}

features_option() {
    local FILTERED=()
    for FEAT in "${FEATURE_FLAGS[@]}"; do
        [[ "${FEAT}" != "" ]] && FILTERED+=("${FEAT}")
    done
    if ((${#FILTERED[@]} != 0)); then
        echo "--features $(_join_by , "${FILTERED[@]}")"
    fi
}