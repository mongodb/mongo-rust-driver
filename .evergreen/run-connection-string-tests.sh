#!/bin/bash

set -o errexit
set -o xtrace
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

FEATURE_FLAGS+=("aws-auth")

set +o errexit

cargo_test spec::auth spec.xml
cargo_test uri_options uri_options.xml
cargo_test connection_string connection_string.xml

merge-junit -o results.xml spec.xml uri_options.xml connection_string.xml

exit ${CARGO_RESULT}
