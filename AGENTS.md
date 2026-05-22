# AGENTS.md - Rust Driver

## Overview
The Rust driver for MongoDB.

## Tech Stack
- Tokio async runtime
- rustls or openssl for TLS
- hickory for DNS
- cargo-nextest for testing

## Project Structure
- `driver/` - the driver crate
- `macros/` - an internal crate with macros used in the driver code
- `benchmarks/` - benchmark suite
- `spec/` - specification test files for unified or legacy test runners
- `docs/tour.md` - code tour for onboarding
- `docs/manual/` - stub for a manual (moved to official docs site)
- `etc/` - random useful tools
- `bevy/` - crate to integrate MongoDB support into the Bevy engine

## Features
The driver provides a lot of optional functionality.  When checking compilation or running tests, make sure the corresponding features are enabled; see `driver/Cargo.toml` or the feature flags section of `driver/README.md` for details.

## Commands
- Build: `cargo build`
- Run all tests: `cargo nextest run`
- Run a single test: `cargo nextest run path::to::test_fn`

## Testing
- A local mongod instance is always available for the tests that need it (most of them).
- When running unified tests, the runner can be restricted to a single file using the `TEST_FILE` env var (just the filename, no path), or a single test using `TEST_DESCRIPTION`.
- Some tests require additional setup (auxiliary servers, etc.) and have a module path that includes `skip_local`.  The cargo-nextest config is set to skip those by default.  Don't try to run them without knowing what they need.
