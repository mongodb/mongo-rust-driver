#!/bin/bash
set -o xtrace   # Write all commands first to stderr
set -o errexit  # Exit the script with error if any of the commands fail

MONGODB_URI="$1"

MONGODB_URI=$MONGODB_URI /root/src/aws-ecs-test
