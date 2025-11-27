#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/features.sh

FEATURE_FLAGS+=("${STANDARD_FEATURES[@]}")

cargo nextest archive --workspace $(features_option) --archive-file nextest-archive.tar.zst