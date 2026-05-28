#!/usr/bin/env bash

set -e

if [ ! -f "driver/Cargo.toml" ]; then
    echo "This script must be run from the repository root."
    exit 1
fi

if ! command -v cargo-cyclonedx > /dev/null 2>&1; then
    echo "cargo-cyclonedx is required; install with \`cargo install cargo-cyclonedx\`"
    exit 1
fi

if ! command -v cyclonedx > /dev/null 2>&1; then
    echo "cyclonedx CLI is required; binaries are available at https://github.com/CycloneDX/cyclonedx-cli/releases/"
    exit 1
fi

pushd etc/update_version
cargo run -- "$@"
popd

RUST_LOG=cargo_cyclonedx=info cargo cyclonedx --manifest-path driver/Cargo.toml --spec-version 1.5 -vv --format json --override-filename sbom --all-features
cp driver/sbom.json sbom.json
# Clean up workspace member SBOMs - we only want the driver SBOM
rm -f driver/sbom.json macros/sbom.json benchmarks/sbom.json etc/update_version/sbom.json

cyclonedx validate --input-file sbom.json --fail-on-errors