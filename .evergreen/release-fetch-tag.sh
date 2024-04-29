#!/bin/bash

set -o errexit

if [[ -z "$GIT_TAG" ]]; then
  echo >&2 "\$GIT_TAG must be set to the git tag of the release"
  exit 1
fi

git fetch origin tag $GIT_TAG --no-tags
git checkout $GIT_TAG
