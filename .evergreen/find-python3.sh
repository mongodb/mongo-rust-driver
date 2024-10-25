#!/bin/bash

source ./.evergreen/env.sh
source ${DRIVERS_TOOLS}/.evergreen/find-python3.sh
PYTHON3=$(find_python3)

cat <<EOT >python3.yml
PYTHON3: "${PYTHON3}
EOT