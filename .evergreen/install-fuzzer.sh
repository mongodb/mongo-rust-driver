#!/bin/bash

set -o errexit

source ./.evergreen/env.sh

cargo install cargo-fuzz
