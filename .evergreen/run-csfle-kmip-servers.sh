#!/bin/bash

cd ${DRIVERS_TOOLS}/.evergreen/csfle
. ./activate_venv.sh
# TMPDIR is required to avoid "AF_UNIX path too long" errors.
export TMPDIR="$(dirname ${DRIVERS_TOOLS})"

kmstlsvenv/bin/python kms_kmip_server.py &
kmstlsvenv/bin/python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/expired.pem --port 9000 &
kmstlsvenv/bin/python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/wrong-host.pem --port 9001 &
kmstlsvenv/bin/python -u kms_http_server.py --ca_file ../x509gen/ca.pem --cert_file ../x509gen/server.pem --port 9002 --require_client_cert