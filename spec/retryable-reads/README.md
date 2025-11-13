# Retryable Reads Tests

______________________________________________________________________

## Introduction

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
retryable reads. These tests utilize the [Unified Test Format](../../unified-test-format/unified-test-format.md).

Several prose tests, which are not easily expressed in YAML, are also presented in this file. Those tests will need to
be manually implemented by each driver.

## Prose Tests

### 1. PoolClearedError Retryability Test

This test will be used to ensure drivers properly retry after encountering PoolClearedErrors. It MUST be implemented by
any driver that implements the CMAP specification. This test requires MongoDB 4.2.9+ for `blockConnection` support in
the failpoint.

1. Create a client with maxPoolSize=1 and retryReads=true. If testing against a sharded deployment, be sure to connect
    to only a single mongos.

2. Enable the following failpoint:

    ```javascript
    {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
            failCommands: ["find"],
            errorCode: 91,
            blockConnection: true,
            blockTimeMS: 1000
        }
    }
    ```

3. Start two threads and attempt to perform a `findOne` simultaneously on both.

4. Verify that both `findOne` attempts succeed.

5. Via CMAP monitoring, assert that the first check out succeeds.

6. Via CMAP monitoring, assert that a PoolClearedEvent is then emitted.

7. Via CMAP monitoring, assert that the second check out then fails due to a connection error.

8. Via Command Monitoring, assert that exactly three `find` CommandStartedEvents were observed in total.

9. Disable the failpoint.

### 2. Retrying Reads in a Sharded Cluster

These tests will be used to ensure drivers properly retry reads on a different mongos.

Note: this test cannot reliably distinguish "retry on a different mongos due to server deprioritization" (the behavior
intended to be tested) from "retry on a different mongos due to normal SDAM behavior of randomized suitable server
selection". Verify relevant code paths are correctly executed by the tests using external means such as a logging,
debugger, code coverage tool, etc.

#### 2.1 Retryable Reads Are Retried on a Different mongos When One is Available

This test MUST be executed against a sharded cluster that has at least two mongos instances, supports `retryReads=true`,
and has enabled the `configureFailPoint` command (MongoDB 4.2+).

1. Create two clients `s0` and `s1` that each connect to a single mongos from the sharded cluster. They must not connect
    to the same mongos.

2. Configure the following fail point for both `s0` and `s1`:

    ```javascript
    {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
            failCommands: ["find"],
            errorCode: 6
        }
    }
    ```

3. Create a client `client` with `retryReads=true` that connects to the cluster using the same two mongoses as `s0` and
    `s1`.

4. Enable failed command event monitoring for `client`.

5. Execute a `find` command with `client`. Assert that the command failed.

6. Assert that two failed command events occurred. Assert that both events occurred on different mongoses.

7. Disable the fail point on both `s0` and `s1`.

#### 2.2 Retryable Reads Are Retried on the Same mongos When No Others are Available

This test MUST be executed against a sharded cluster that supports `retryReads=true` and has enabled the
`configureFailPoint` command (MongoDB 4.2+).

1. Create a client `s0` that connects to a single mongos from the cluster.

2. Configure the following fail point for `s0`:

    ```javascript
    {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
            failCommands: ["find"],
            errorCode: 6
        }
    }
    ```

3. Create a client `client` with `directConnection=false` (when not set by default) and `retryReads=true` that connects
    to the cluster using the same single mongos as `s0`.

4. Enable succeeded and failed command event monitoring for `client`.

5. Execute a `find` command with `client`. Assert that the command succeeded.

6. Assert that exactly one failed command event and one succeeded command event occurred. Assert that both events
    occurred on the same mongos.

7. Disable the fail point on `s0`.

## Changelog

- 2024-04-30: Migrated from reStructuredText to Markdown.

- 2024-03-06: Convert legacy retryable reads tests to unified format.

- 2024-02-21: Update mongos redirection prose tests to workaround SDAM behavior preventing execution of deprioritization
    code paths.

- 2023-08-26: Add prose tests for retrying in a sharded cluster.

- 2022-04-22: Clarifications to `serverless` and `useMultipleMongoses`.

- 2022-01-10: Create legacy and unified subdirectories for new unified tests

- 2021-08-27: Clarify behavior of `useMultipleMongoses` for `LoadBalanced` topologies.

- 2019-03-19: Add top-level `runOn` field to denote server version and/or topology requirements requirements for the
    test file. Removes the `minServerVersion` and `topology` top-level fields, which are now expressed within `runOn`
    elements.

    Add test-level `useMultipleMongoses` field.

- 2020-09-16: Suggest lowering heartbeatFrequencyMS in addition to minHeartbeatFrequencyMS.

- 2021-03-23: Add prose test for retrying PoolClearedErrors

- 2021-04-29: Add `load-balanced` to test topology requirements.
