#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/features.sh

FEATURE_FLAGS+=("${STANDARD_FEATURES[@]}")

add_conditional_features

if [[ "$IN_USE_ENCRYPTION" = true ]]; then
    FEATURE_FLAGS+=("in-use-encryption" "azure-kms" "text-indexes-unstable" "aws-auth")
fi

cargo nextest archive --workspace $(features_option) --archive-file nextest-archive.tar.zst