set -o errexit
set -o pipefail

source ./.evergreen/env.sh
source .evergreen/cargo-test.sh

set -o xtrace

set +o errexit

FEATURE_FLAGS+=("tokio-runtime")

cargo_test test::spec::oidc prose.xml

junit-report-merger results.xml prose.xml

exit ${CARGO_RESULT}
