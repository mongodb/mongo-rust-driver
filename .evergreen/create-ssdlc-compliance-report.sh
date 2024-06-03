#!/bin/bash

set -o errexit
set -o xtrace

REPORT_FILE=".evergreen/${CRATE_VERSION}-ssdlc-compliance-report.md"
SED_REPLACE="s/RELEASE_VERSION/${CRATE_VERSION}/g"

sed $SED_REPLACE .evergreen/ssdlc-compliance-report-template.md > ${REPORT_FILE}
