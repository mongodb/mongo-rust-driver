#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

cargo test --doc --package mongodb --all-features