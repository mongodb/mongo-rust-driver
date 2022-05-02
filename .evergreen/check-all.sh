#!/bin/sh

set -o errexit

# Note: This script must run from root, or it won't check src/ files correctly.
EVERGREEN_DIR=".evergreen"

bash "${EVERGREEN_DIR}/check-clippy.sh"
bash "${EVERGREEN_DIR}/check-rustdoc.sh"
bash "${EVERGREEN_DIR}/check-rustfmt.sh"
bash "${EVERGREEN_DIR}/check-cargo-deny.sh"
