#!/bin/bash

set -o errexit

if [[ -z "$DRIVERS_TOOLS" ]]; then
    echo >&2 "\$DRIVERS_TOOLS must be set"
    exit 1
fi

rm -rf $DRIVERS_TOOLS
git clone https://github.com/mongodb-labs/drivers-evergreen-tools.git $DRIVERS_TOOLS