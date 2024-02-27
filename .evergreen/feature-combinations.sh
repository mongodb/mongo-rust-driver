#!/bin/bash

# Only default features.
export NO_FEATURES=''
# All additional features that do not conflict with the default features. New features added to the library should also be added to this list.
export ADDITIONAL_FEATURES='--features sync,zstd-compression,snappy-compression,zlib-compression,openssl-tls,aws-auth,tracing-unstable,in-use-encryption-unstable,azure-kms'

# Array of feature combinations that, in total, provides complete coverage of the driver.
# This is useful for linting tasks where we want to get coverage of all features.
# Since some of our features are mutually exclusive we cannot just use --all-features.
export FEATURE_COMBINATIONS=(
  "$NO_FEATURES"
  "$ADDITIONAL_FEATURES"
)
