#!/bin/bash

# Array of feature combinations that, in total, provides complete coverage of the driver.
# This is useful for linting tasks where we want to get coverage of all features.
# Since some of our features are mutually exclusive we cannot just use --all-features.
export FEATURE_COMBINATIONS=(
    '' # default features
    '--no-default-features --features async-std-runtime,sync' # features that conflict w/ default features
    # TODO: RUST-1490 add csfle feature
    '--features tokio-sync,zstd-compression,snappy-compression,zlib-compression,openssl-tls,aws-auth,tracing-unstable' # additive features
)
