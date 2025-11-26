set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

# Feature flags are set when the archive is built
FEATURE_FLAGS=()
CARGO_OPTIONS+=("--archive-file" "nextest-archive.tar.zst" "--workspace-remap" "$(pwd)")

set +o errexit

cargo_test ""

exit $CARGO_RESULT