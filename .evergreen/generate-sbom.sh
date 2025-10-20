#!/usr/bin/env bash
set -euo pipefail

# generate-sbom.sh
# Purpose: Produce a CycloneDX SBOM for the Rust driver via cdxgen and enrich it with Parlay.
# Usage: bash .evergreen/generate-sbom.sh [--quiet]

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUIET=0
for arg in "$@"; do
  case "$arg" in
    --quiet) QUIET=1 ;;
  esac
done

log() { [ "$QUIET" -eq 1 ] || echo "$*" >&2; }
err() { echo "ERROR: $*" >&2; }

require_cmd() {
  local c="$1"; shift || true
  if ! command -v "$c" >/dev/null 2>&1; then
    err "Missing required command: $c. $*"; exit 1;
  fi
}

log "Checking prerequisites"
require_cmd node "Install Node.js >= 20 (https://nodejs.org/)"
require_cmd npm "Install Node.js/npm"
require_cmd wget "Install wget (e.g., sudo apt-get install -y wget)"
require_cmd jq "Install jq (e.g., sudo apt-get install -y jq)"
if ! npx --no-install cdxgen --version >/dev/null 2>&1; then
  log "Installing @cyclonedx/cdxgen"
  npm install @cyclonedx/cdxgen >/dev/null 2>&1 || { err "Failed to install cdxgen"; exit 1; }
fi

# Install parlay if missing
if ! command -v parlay >/dev/null 2>&1; then
  log "Installing Parlay"
  ARCH="$(uname -m)"; OS="$(uname -s)"
  # Map architecture names
  case "$ARCH" in
    x86_64|amd64) ARCH_DL="x86_64" ;;
    aarch64|arm64) ARCH_DL="aarch64" ;;
    *) err "Unsupported architecture for parlay: $ARCH"; exit 1;;
  esac
  if [ "$OS" != "Linux" ]; then
    err "Parlay install script currently supports Linux only"; exit 1;
  fi
  PARLAY_TAR="parlay_Linux_${ARCH_DL}.tar.gz"
  wget -q "https://github.com/snyk/parlay/releases/latest/download/${PARLAY_TAR}" || { err "Failed to download Parlay"; exit 1; }
  tar -xzf "$PARLAY_TAR"
  chmod +x parlay || true
  mv parlay "$SCRIPT_DIR/parlay-bin" || true
  export PATH="$SCRIPT_DIR:$PATH"
fi

# Extract crate metadata from Cargo.toml
CRATE_NAME="$(grep -E '^name\s*=\s*"' Cargo.toml | head -1 | sed -E 's/name\s*=\s*"([^"]+)"/\1/')"
CRATE_VERSION="$(grep -E '^version\s*=\s*"' Cargo.toml | head -1 | sed -E 's/version\s*=\s*"([^"]+)"/\1/')"
CRATE_LICENSE="$(grep -E '^license\s*=\s*"' Cargo.toml | head -1 | sed -E 's/license\s*=\s*"([^"]+)"/\1/')"
if [ -z "$CRATE_NAME" ] || [ -z "$CRATE_VERSION" ]; then
  err "Failed to parse crate name/version from Cargo.toml"; exit 1;
fi
log "Crate: $CRATE_NAME v$CRATE_VERSION (license: $CRATE_LICENSE)"

SBOM_FILE="sbom.${CRATE_NAME}@v${CRATE_VERSION}.cdxgen.json"
ENRICHED_SBOM_FILE="sbom.${CRATE_NAME}@v${CRATE_VERSION}.cdxgen.parlay.json"

log "Generating SBOM: $SBOM_FILE"
npx cdxgen --type cargo --output "$SBOM_FILE" >/dev/null || { err "cdxgen failed"; exit 1; }
mv "$SBOM_FILE" "$SBOM_FILE.raw"
jq . "$SBOM_FILE.raw" > "$SBOM_FILE" || { err "Failed to pretty-print SBOM"; exit 1; }
rm -f "$SBOM_FILE.raw"

grep -q 'CycloneDX' "$SBOM_FILE" || { err "CycloneDX marker missing in $SBOM_FILE"; exit 1; }
test $(stat -c%s "$SBOM_FILE") -gt 1000 || { err "SBOM file too small (<1000 bytes)"; exit 1; }

log "Enriching SBOM: $ENRICHED_SBOM_FILE"
parlay ecosystems enrich "$SBOM_FILE" > "$ENRICHED_SBOM_FILE.raw" || { err "Parlay enrichment failed"; exit 1; }
jq . "$ENRICHED_SBOM_FILE.raw" > "$ENRICHED_SBOM_FILE" || { err "Failed to pretty-print enriched SBOM"; exit 1; }
rm -f "$ENRICHED_SBOM_FILE.raw"

log "Done"
echo "SBOM: $SBOM_FILE"
echo "SBOM (enriched): $ENRICHED_SBOM_FILE"
