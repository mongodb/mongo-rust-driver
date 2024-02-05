#!/bin/bash

# Needed for cargo-lambda
pip3 install ziglang

source ${PROJECT_DIRECTORY}/.cargo/env
rustup default stable

${PROJECT_DIRECTORY}/.evergreen/run-deployed-lambda-aws-tests.sh