#!/bin/bash

. ${DRIVERS_TOOLS}/.evergreen/find-python3.sh
PYTHON=$(find_python3)

function prepend() { while read line; do echo "${1}${line}"; done; }

cd ${DRIVERS_TOOLS}/.evergreen/csfle
${PYTHON} bottle.py fake_azure:imds -b localhost:${AZURE_IMDS_MOCK_PORT} &