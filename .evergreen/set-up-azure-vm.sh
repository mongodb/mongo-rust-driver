#!/bin/bash

set -o xtrace
set -o errexit

source ${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh azurekms

AZUREKMS_PUBLICKEYPATH="/tmp/testazurekms_publickey"
AZUREKMS_PRIVATEKEYPATH="/tmp/testazurekms_privatekey"

set +o xtrace
echo ${AZUREKMS_PUBLICKEY} > ${AZUREKMS_PUBLICKEYPATH}
echo ${AZUREKMS_PRIVATEKEY} > ${AZUREKMS_PRIVATEKEYPATH}
set -o xtrace

# Set 600 permissions on private key file. Otherwise ssh / scp may error with permissions "are too open".
chmod 600 ${AZUREKMS_PRIVATEKEYPATH}

cat <<EOT > azure-keys.yml
AZUREKMS_PUBLICKEYPATH: ${AZUREKMS_PUBLICKEYPATH}
AZUREKMS_PRIVATEKEYPATH: ${AZUREKMS_PRIVATEKEYPATH}
EOT

$DRIVERS_TOOLS/.evergreen/csfle/azurekms/create-and-setup-vm.sh
