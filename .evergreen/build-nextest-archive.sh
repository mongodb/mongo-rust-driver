#!/bin/bash

set -o errexit
set -o pipefail

source .evergreen/env.sh

cargo nextest archive --workspace --all-features --archive-file nextest-archive.tar.zst