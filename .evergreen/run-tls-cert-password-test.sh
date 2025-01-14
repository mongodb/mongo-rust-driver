#!/bin/bash

set -o errexit

source .evergreen/env.sh

cd etc/tls_cert_password
cargo run