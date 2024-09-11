#!/bin/bash

set -o errexit
set -o xtrace
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("aws-auth")

set +o errexit

cargo_test spec::auth
cargo_test uri_options
cargo_test connection_string

exit ${CARGO_RESULT}
