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

add_conditional_features() {
    if [ "$OPENSSL" = true ]; then
        FEATURE_FLAGS+=("openssl-tls")
    fi

    if [ "$ZSTD" = true ]; then
        FEATURE_FLAGS+=("zstd-compression")
    fi

    if [ "$ZLIB" = true ]; then
        FEATURE_FLAGS+=("zlib-compression")
    fi

    if [ "$SNAPPY" = true ]; then
        FEATURE_FLAGS+=("snappy-compression")
    fi
}