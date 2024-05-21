#!/bin/bash

set -o errexit
set +x

echo "${ARTIFACTORY_PASSWORD}" | docker login --password-stdin --username ${ARTIFACTORY_USERNAME} artifactory.corp.mongodb.com

echo "GRS_CONFIG_USER1_USERNAME=${GARASIGN_USERNAME}" >> "signing-envfile"
echo "GRS_CONFIG_USER1_PASSWORD=${GARASIGN_PASSWORD}" >> "signing-envfile"

docker run \
  --env-file=signing-envfile \
  --rm \
  -v $(pwd):$(pwd) \
  -w $(pwd) \
  artifactory.corp.mongodb.com/release-tools-container-registry-local/garasign-gpg \
  /bin/bash -c "gpgloader && gpg --yes -v --armor -o mongodb-${CRATE_VERSION}.sig --detach-sign target/package/mongodb-${CRATE_VERSION}.crate"

docker run \
  --env-file=signing-envfile \
  --rm \
  -v $(pwd):$(pwd) \
  -w $(pwd) \
  artifactory.corp.mongodb.com/release-tools-container-registry-local/garasign-gpg \
  /bin/bash -c "gpgloader && gpg --yes -v --armor -o mongodb-internal-macros-${CRATE_VERSION}.sig --detach-sign macros/target/package/mongodb-internal-macros-${CRATE_VERSION}.crate"
