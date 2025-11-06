#!/usr/bin/env bash
set -euo pipefail

# Ephemeral SBOM generator (Rust) using mise + cargo-cyclonedx.
# Environment overrides:
#  MISE_RUST_VERSION  Rust version (default nightly)
#  SBOM_OUT           Output filename (default sbom.json)
#
# Usage: bash .evergreen/generate-sbom.sh

RUST_VERSION="${MISE_RUST_VERSION:-latest}"
JQ_VERSION="${JQ_VERSION:-latest}"
OUT_JSON="${SBOM_OUT:-sbom.json}"

log() { printf '\n[sbom] %s\n' "$*"; }

# Ensure mise is available (installed locally in $HOME) and PATH includes shims.

ensure_mise() {
  # Installer places binary in ~/.local/bin/mise by default.
  if ! command -v mise >/dev/null 2>&1; then
    log "Installing mise"
    curl -fsSL https://mise.run | bash >/dev/null 2>&1 || { log "mise install script failed"; exit 1; }
  fi
  # Ensure ~/.local/bin precedes so 'mise' is found even if shims absent.
  export PATH="$HOME/.local/bin:$HOME/.local/share/mise/shims:$HOME/.local/share/mise/bin:$PATH"
  if ! command -v mise >/dev/null 2>&1; then
    log "mise not found on PATH after install"; ls -al "$HOME/.local/bin" || true; exit 1
  fi
}

## resolve_toolchain_flags
# Returns space-separated tool@version specs required for SBOM generation.
resolve_toolchain_flags() {
  printf 'rust@%s jq@%s' "$RUST_VERSION" "$JQ_VERSION"
}

## prepare_exec_prefix
# Builds the mise exec prefix for ephemeral command runs.
prepare_exec_prefix() {
  local tools
  tools="$(resolve_toolchain_flags)"
  echo "mise exec $tools --"
}

## ensure_cargo_cyclonedx
# Installs cargo-cyclonedx if not available.
ensure_cargo_cyclonedx() {
  if ! mise exec rust@"$RUST_VERSION" -- cargo cyclonedx --version >/dev/null 2>&1; then
    log "Installing cargo-cyclonedx"
    mise exec rust@"$RUST_VERSION" -- cargo install cargo-cyclonedx || { log "Failed to install cargo-cyclonedx"; exit 1; }
  fi
}

## generate_sbom
# Executes cargo-cyclonedx to generate SBOM.
generate_sbom() {
  log "Generating SBOM using cargo-cyclonedx"
  local exec_prefix
  exec_prefix="$(prepare_exec_prefix)"
  $exec_prefix cargo cyclonedx -vv --format json --override-filename sbom || {
    log "SBOM generation failed"; exit 1; }
  log "SBOM generated"
}

## install_toolchains
# Installs required runtime versions into the local mise cache unconditionally.
# (mise skips download if already present.)
install_toolchains() {
  local tools
  tools="$(resolve_toolchain_flags)"
  log "Installing toolchains: $tools"
  mise install $tools >/dev/null
}

## format_sbom
# Formats the SBOM JSON with jq (required). Exits non-zero if formatting fails.
format_sbom() {
  log "Formatting SBOM via jq@$JQ_VERSION"
  if ! mise exec jq@"$JQ_VERSION" -- jq . "$OUT_JSON" > "$OUT_JSON.tmp" 2>/dev/null; then
    log "jq formatting failed"; return 1
  fi
  mv "$OUT_JSON.tmp" "$OUT_JSON"
}

main() {
  ensure_mise
  install_toolchains
  ensure_cargo_cyclonedx
  generate_sbom
  format_sbom
}

main "$@"
