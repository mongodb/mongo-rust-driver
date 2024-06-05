#!/usr/bin/env bash

set -o errexit
set -o pipefail

source .evergreen/env.sh
source .evergreen/cargo-test.sh

export SERVERLESS="serverless"

set +o errexit

cargo_test test::spec::crud crud.xml
cargo_test test::spec::retryable_reads retryable_reads.xml
cargo_test test::spec::retryable_writes retryable_writes.xml
cargo_test test::spec::versioned_api versioned_api.xml
cargo_test test::spec::sessions sessions.xml
cargo_test test::spec::transactions transactions.xml
cargo_test test::spec::load_balancers load_balancers.xml
cargo_test test::cursor cursor.xml
cargo_test test::spec::collection_management coll.xml
cargo_test test::spec::command_monitoring_unified monitoring.xml

merge-junit -o results.xml crud.xml retryable_reads.xml retryable_writes.xml versioned_api.xml sessions.xml transactions.xml load_balancers.xml cursor.xml coll.xml monitoring.xml

exit $CARGO_RESULT
