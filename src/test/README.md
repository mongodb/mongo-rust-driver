# Test development guide

This document is a work-in-progress guide for developing tests for the driver.

## Filtering tests
Tests that require any additional setup are filtered using cargo nextest's [filterset](https://nexte.st/docs/filtersets/). The driver uses two filters (configured in [nextest.toml](../../.config/nextest.toml)):

- `skip_local`: skips filtered tests when running locally.
- `skip_ci`: skips filtered tests when running locally and in CI.

Filtered tests should be organized into modules with the desired filter at the end of the module name (e.g. `search_index_skip_ci`).

Filters can be bypassed locally by passing `--ignore-default-filter` to `cargo nextest run`. Filters can be bypassed in CI by adding the following to the script used to run the test:

```
source .evergreen/cargo-test.sh
CARGO_OPTIONS+=("--ignore-default-filter")
```
