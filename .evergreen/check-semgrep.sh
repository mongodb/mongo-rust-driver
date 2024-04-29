#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

. ${DRIVERS_TOOLS}/.evergreen/find-python3.sh
PYTHON=$(find_python3)

if [[ -f "semgrep/bin/activate" ]]; then
    echo 'using existing virtualenv'
    . semgrep/bin/activate
else
    echo 'Creating new virtualenv'  
    ${PYTHON} -m venv semgrep
    echo 'Activating new virtualenv'
    . semgrep/bin/activate
    python3 -m pip install semgrep
fi

# Generate a SARIF report
semgrep --config p/rust --sarif > mongo-rust-driver.json.sarif
# And human-readable output
semgrep --config p/rust --error