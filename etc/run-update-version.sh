#!/usr/bin/env bash

set -e

if [ ! -f "driver/Cargo.toml" ]; then
    echo "This script must be run from the repository root."
    exit 1
fi

if ! command -v sbom-attest > /dev/null 2>&1; then
    echo "sbom-attest is required; install with \`cargo install sbom-attest\`"
    exit 1
fi

if ! command -v cyclonedx > /dev/null 2>&1; then
    echo "cyclonedx CLI is required; binaries are available at https://github.com/CycloneDX/cyclonedx-cli/releases/"
    exit 1
fi

pushd etc/update_version
cargo run -- "$@"
popd

# Dependencies and features are resolved for the driver alone (as a crate depending on it would
# get them), so the driver's dev-dependencies and the other workspace members are not included.
sbom-attest generate --manifest-path driver/Cargo.toml --package mongodb --all-features --author "MongoDB, Inc." --cdx sbom.json

cyclonedx validate --input-file sbom.json --fail-on-errors