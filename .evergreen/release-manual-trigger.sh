#!/bin/bash

# This should only be used if the normal evergreen automation triggered by the
# tag push has failed.  This will manually trigger the same evergreen workflow
# that the tag push would have.

if [[ -z "$TAG" ]]; then
  echo >&2 "\$TAG must be set to the git tag of the release"
  exit 1
fi
if [[ "$CONFIRM" != "YES" ]]; then
  echo >&2 "THIS ACTION IS IRREVOCABLE.  Set \$CONFIRM to YES to validate that you really want to release a new version of the driver."
  exit 1
fi

evergreen patch --path .evergreen/releases.yml -t publish-release -v all -u -p mongo-rust-driver-current --browse --param triggered_by_git_tag=${TAG}