#!/bin/bash

set -o errexit
set -o xtrace

source ./.evergreen/env.sh
rustup update $RUST_VERSION

# pin dependencies who have bumped their MSRVs to > ours in recent releases.
if [  "$MSRV" = "true" ]; then
    cp .evergreen/MSRV-Cargo.lock Cargo.lock
fi

source ./.evergreen/feature-combinations.sh

# build with all available features to ensure all optional dependencies are brought in too.
for ((i = 0; i < ${#FEATURE_COMBINATIONS[@]}; i++)); do
    rustup run $RUST_VERSION cargo build ${FEATURE_COMBINATIONS[$i]}
done
