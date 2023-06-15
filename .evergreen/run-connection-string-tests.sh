#!/bin/bash

set -o errexit
set -o xtrace
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("aws-auth")

set +o errexit

echo spec
cargo_test spec::auth > spec.xml
echo options
cargo_test uri_options > uri_options.xml
echo string
cargo_test connection_string > connection_string.xml
echo merge
junit-report-merger results.xml spec.xml uri_options.xml connection_string.xml
echo done ${CARGO_RESULT}
exit ${CARGO_RESULT}