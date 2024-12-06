#!/usr/bin/env bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

export SERVERLESS="serverless"

set +o errexit

cargo_test test::spec::crud
cargo_test test::spec::retryable_reads
cargo_test test::spec::retryable_writes
cargo_test test::spec::versioned_api
cargo_test test::spec::sessions
cargo_test test::spec::transactions
cargo_test test::spec::load_balancers
cargo_test test::cursor
cargo_test test::spec::collection_management
cargo_test test::spec::command_monitoring::command_monitoring_unified

exit $CARGO_RESULT
