#!/bin/bash

set -o xtrace
set -o errexit

source ${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh azurekms

set +o xtrace
echo ${AZUREKMS_PUBLICKEY} > "/tmp/testazurekms_publickey"
echo ${AZUREKMS_PRIVATEKEY} > "/tmp/testazurekms_privatekey"
set -o xtrace

# Set 600 permissions on private key file. Otherwise ssh / scp may error with permissions "are too open".
chmod 600 /tmp/testazurekms_privatekey

$DRIVERS_TOOLS/.evergreen/csfle/azurekms/create-and-setup-vm.sh
