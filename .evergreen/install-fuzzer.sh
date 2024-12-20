#!/bin/bash

set -o errexit

. ~/.cargo/env

cargo install cargo-fuzz
