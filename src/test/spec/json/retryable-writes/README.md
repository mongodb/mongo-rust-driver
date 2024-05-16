# Retryable Write Tests

______________________________________________________________________

## Introduction

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
retryable writes. These tests utilize the [Unified Test Format](../../unified-test-format/unified-test-format.md).

Several prose tests, which are not easily expressed in YAML, are also presented in this file. Those tests will need to
be manually implemented by each driver.

Tests will require a MongoClient created with options defined in the tests. Integration tests will require a running
MongoDB cluster with server versions 3.6.0 or later. The `{setFeatureCompatibilityVersion: 3.6}` admin command will also
need to have been executed to enable support for retryable writes on the cluster. Some tests may have more stringent
version requirements depending on the fail points used.

## Use as Integration Tests

Integration tests are expressed in YAML and can be run against a replica set or sharded cluster as denoted by the
top-level `runOn` field. Tests that rely on the `onPrimaryTransactionalWrite` fail point cannot be run against a sharded
cluster because the fail point is not supported by mongos.

The tests exercise the following scenarios:

- Single-statement write operations
  - Each test expecting a write result will encounter at-most one network error for the write command. Retry attempts
    should return without error and allow operation to succeed. Observation of the collection state will assert that the
    write occurred at-most once.
  - Each test expecting an error will encounter successive network errors for the write command. Observation of the
    collection state will assert that the write was never committed on the server.
- Multi-statement write operations
  - Each test expecting a write result will encounter at-most one network error for some write command(s) in the batch.
    Retry attempts should return without error and allow the batch to ultimately succeed. Observation of the collection
    state will assert that each write occurred at-most once.
  - Each test expecting an error will encounter successive network errors for some write command in the batch. The batch
    will ultimately fail with an error, but observation of the collection state will assert that the failing write was
    never committed on the server. We may observe that earlier writes in the batch occurred at-most once.

We cannot test a scenario where the first and second attempts both encounter network errors but the write does actually
commit during one of those attempts. This is because (1) the fail point only triggers when a write would be committed
and (2) the skip and times options are mutually exclusive. That said, such a test would mainly assert the server's
correctness for at-most once semantics and is not essential to assert driver correctness.

## Split Batch Tests

The YAML tests specify bulk write operations that are split by command type (e.g. sequence of insert, update, and delete
commands). Multi-statement write operations may also be split due to `maxWriteBatchSize`, `maxBsonObjectSize`, or
`maxMessageSizeBytes`.

For instance, an insertMany operation with five 10 MiB documents executed using OP_MSG payload type 0 (i.e. entire
command in one document) would be split into five insert commands in order to respect the 16 MiB `maxBsonObjectSize`
limit. The same insertMany operation executed using OP_MSG payload type 1 (i.e. command arguments pulled out into a
separate payload vector) would be split into two insert commands in order to respect the 48 MB `maxMessageSizeBytes`
limit.

Noting when a driver might split operations, the `onPrimaryTransactionalWrite` fail point's `skip` option may be used to
control when the fail point first triggers. Once triggered, the fail point will transition to the `alwaysOn` state until
disabled. Driver authors should also note that the server attempts to process all documents in a single insert command
within a single commit (i.e. one insert command with five documents may only trigger the fail point once). This behavior
is unique to insert commands (each statement in an update and delete command is processed independently).

If testing an insert that is split into two commands, a `skip` of one will allow the fail point to trigger on the second
insert command (because all documents in the first command will be processed in the same commit). When testing an update
or delete that is split into two commands, the `skip` should be set to the number of statements in the first command to
allow the fail point to trigger on the second command.

## Command Construction Tests

Drivers should also assert that command documents are properly constructed with or without a transaction ID, depending
on whether the write operation is supported.
[Command Logging and Monitoring](../../command-logging-and-monitoring/command-logging-and-monitoring.rst) may be used to
check for the presence of a `txnNumber` field in the command document. Note that command documents may always include an
`lsid` field per the [Driver Session](../../sessions/driver-sessions.md) specification.

These tests may be run against both a replica set and shard cluster.

Drivers should test that transaction IDs are never included in commands for unsupported write operations:

- Write commands with unacknowledged write concerns (e.g. `{w: 0}`)
- Unsupported single-statement write operations
  - `updateMany()`
  - `deleteMany()`
- Unsupported multi-statement write operations
  - `bulkWrite()` that includes `UpdateMany` or `DeleteMany`
- Unsupported write commands
  - `aggregate` with write stage (e.g. `$out`, `$merge`)

Drivers should test that transactions IDs are always included in commands for supported write operations:

- Supported single-statement write operations
  - `insertOne()`
  - `updateOne()`
  - `replaceOne()`
  - `deleteOne()`
  - `findOneAndDelete()`
  - `findOneAndReplace()`
  - `findOneAndUpdate()`
- Supported multi-statement write operations
  - `insertMany()` with `ordered=true`
  - `insertMany()` with `ordered=false`
  - `bulkWrite()` with `ordered=true` (no `UpdateMany` or `DeleteMany`)
  - `bulkWrite()` with `ordered=false` (no `UpdateMany` or `DeleteMany`)

## Prose Tests

The following tests ensure that retryable writes work properly with replica sets and sharded clusters.

1. Test that retryable writes raise an exception when using the MMAPv1 storage engine. For this test, execute a write
   operation, such as `insertOne`, which should generate an exception. Assert that the error message is the replacement
   error message:

   ```
   This MongoDB deployment does not support retryable writes. Please add
   retryWrites=false to your connection string.
   ```

   and the error code is 20.

   [!NOTE]
   storage engine in use MAY skip this test for sharded clusters, since `mongos` does not report this information in its
   `serverStatus` response.
