set -o errexit
set -o pipefail

source ./.evergreen/env.sh

set -o xtrace

OPTIONS="-- -Z unstable-options --format json --report-time"
CARGO_RESULT=0

cargo_test() {
    RUST_BACKTRACE=1 \
        cargo test $1 ${OPTIONS} | tee /dev/stderr | cargo2junit
    CARGO_RESULT=$(( ${CARGO_RESULT} || $? ))
}

set +o errexit

cargo_test test::spec::oidc > prose.xml

junit-report-merger results.xml prose.xml

exit ${CARGO_RESULT}