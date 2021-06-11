#!/bin/sh

set -o errexit

EVERGREEN_DIR=$(dirname "$0")

bash "${EVERGREEN_DIR}/check-clippy.sh"
bash "${EVERGREEN_DIR}/check-rustdoc.sh"
bash "${EVERGREEN_DIR}/check-rustfmt.sh"
