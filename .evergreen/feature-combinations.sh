#!/bin/bash

# Only default features.
export NO_FEATURES=''
# async-std-related features that conflict with the library's default features.
export ASYNC_STD_FEATURES='--no-default-features --features async-std-runtime,sync'
# All additional features that do not conflict with the default features. New features added to the library should also be added to this list.
export ADDITIONAL_FEATURES='--features tokio-sync,zstd-compression,snappy-compression,zlib-compression,openssl-tls,aws-auth,tracing-unstable,in-use-encryption-unstable,azure-kms'

# Array of feature combinations that, in total, provides complete coverage of the driver.
# This is useful for linting tasks where we want to get coverage of all features.
# Since some of our features are mutually exclusive we cannot just use --all-features.
export FEATURE_COMBINATIONS=(
  "$NO_FEATURES"
  "$ASYNC_STD_FEATURES"
  "$ADDITIONAL_FEATURES"
)
