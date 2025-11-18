#!/bin/bash

set -o errexit
set +x

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 901841024863.dkr.ecr.us-east-1.amazonaws.com

echo "GRS_CONFIG_USER1_USERNAME=${GARASIGN_USERNAME}" >> "signing-envfile"
echo "GRS_CONFIG_USER1_PASSWORD=${GARASIGN_PASSWORD}" >> "signing-envfile"

docker run \
  --env-file=signing-envfile \
  --rm \
  -v $(pwd):$(pwd) \
  -w $(pwd) \
  901841024863.dkr.ecr.us-east-1.amazonaws.com/release-infrastructure/garasign-gpg \
  /bin/bash -c "gpgloader && gpg --yes -v --armor -o mongodb-${CRATE_VERSION}.sig --detach-sign target/package/mongodb-${CRATE_VERSION}.crate"

docker run \
  --env-file=signing-envfile \
  --rm \
  -v $(pwd):$(pwd) \
  -w $(pwd) \
  901841024863.dkr.ecr.us-east-1.amazonaws.com/release-infrastructure/garasign-gpg \
  /bin/bash -c "gpgloader && gpg --yes -v --armor -o mongodb-internal-macros-${CRATE_VERSION}.sig --detach-sign target/package/mongodb-internal-macros-${CRATE_VERSION}.crate"
