#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

use_async_runtime

set +o errexit

cargo_test atlas_connectivity > results.xml

exit $CARGO_RESULT