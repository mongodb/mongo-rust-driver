#!/usr/bin/env bash

source .evergreen/env.sh

set -o xtrace
set +o errexit

cd bevy

cargo nextest run --config-file ../.config/nextest.toml --profile ci
TEST_RESULT=$?
mv -f target/nextest/ci/junit.xml ../results.xml

exit ${TEST_RESULT}
