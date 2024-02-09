#!/bin/bash

# Needed for cargo-lambda
pip3 install ziglang

source ${PROJECT_DIRECTORY}/.cargo/env
rustup default stable

${DRIVERS_TOOLS}/.evergreen/aws_lambda/run-deployed-lambda-aws-tests.sh