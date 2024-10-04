#!/bin/bash

# This script takes an AWS Secrets Manager vault name and a command line
# to execute, e.g.
#
# ./with-secrets drivers/atlas-dev setup-atlas-cluster.sh
#
# It fetches the secrets from the vault, populates the local environment
# variables with those secrets, and then executes the command line.
#
# Secrets are cached based on the name of the vault, so if an earlier
# task has fetched the same vault those secrets will be reused.

vault=$1
shift

if [ -z "${vault}" ] || [ -z "$@" ]; then
    echo "At least two arguments (vault name and command) are required."
    exit 1
fi

vault_cache_key=$(echo "${vault}" | sed -e s/\\\//_/)
vault_cache_file="secrets-${vault_cache_key}.sh"

if [ -f "${vault_cache_file}" ]; then
    # Cached, hooray
    . "${vault_cache_file}"
else
    # Need to actually fetch from the vault
    if [ -z "${DRIVERS_TOOLS}" ]; then
        echo "\$DRIVERS_TOOLS must be set."
        exit 1
    fi
    . "${DRIVERS_TOOLS}/.evergreen/secrets_handling/setup-secrets.sh" "${vault}"
    mv secrets-export.sh "${vault_cache_file}"
fi

exec "$@"