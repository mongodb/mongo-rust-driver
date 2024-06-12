#!/bin/bash

set -o errexit

if [ -t 0 ] ; then
    # Interactive shell
    PYTHON3=${PYTHON3:-"python3"}
else
    # Evergreen run (probably)
    source ./.evergreen/env.sh
    source ${DRIVERS_TOOLS}/.evergreen/find-python3.sh
    PYTHON3=$(find_python3)
fi

if [[ -f "semgrep/bin/activate" ]]; then
    echo 'Using existing virtualenv...'
    . semgrep/bin/activate
else
    echo 'Creating new virtualenv...'
    ${PYTHON3} -m venv semgrep
    echo 'Activating new virtualenv...'
    . semgrep/bin/activate
    echo 'Installing semgrep...'
    python3 -m pip install semgrep
fi

# Show human-readable output
semgrep --config p/rust --error
# Generate a SARIF report
semgrep --config p/rust --quiet --sarif -o sarif.json