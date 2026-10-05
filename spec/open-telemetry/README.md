# OpenTelemetry Tests

______________________________________________________________________

## Testing

### Automated Tests

The YAML and JSON files in this directory are platform-independent tests meant to exercise a driver's implementation of
the OpenTelemetry specification. These tests utilize the
[Unified Test Format](../../unified-test-format/unified-test-format.md).

For each test, create a MongoClient, configure it to enable tracing.

```yaml
createEntities:
  - client:
      id: client0
      observeTracingMessages:
        enableCommandPayload: true
```

These tests require the ability to collect tracing [spans](../open-telemetry.md) data in a structured form as described
in the
[Unified Test Format specification.expectTracingMessages](../../unified-test-format/unified-test-format.md#expectTracingMessages).
For example the Java driver uses [Micrometer](https://jira.mongodb.org/browse/JAVA-5732) to collect tracing spans.

```yaml
expectTracingMessages:
  client: client0
  ignoreExtraSpans: false
  spans:
   ...
```

### Prose Tests

*Test 1: Tracing Enable/Disable via Environment Variable*

1. Set the environment variable `OTEL_#{LANG}_INSTRUMENTATION_MONGODB_ENABLED` to `false`.
2. Create a `MongoClient` without explicitly enabling tracing.
3. Perform a database operation (e.g., `find()` on a test collection).
4. Assert that no OpenTelemetry tracing spans are emitted for the operation.
5. Set the environment variable `OTEL_#{LANG}_INSTRUMENTATION_MONGODB_ENABLED` to `true`.
6. Create a new `MongoClient` without explicitly enabling tracing.
7. Perform the same database operation.
8. Assert that OpenTelemetry tracing spans are emitted for the operation.

*Test 2: Command Payload Emission via Environment Variable*

1. Set the environment variable `OTEL_#{LANG}_INSTRUMENTATION_MONGODB_ENABLED` to `true`.
2. Set the environment variable `OTEL_#{LANG}_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH` to a positive integer
    (e.g., 1024).
3. Create a `MongoClient` without explicitly enabling command payload emission.
4. Perform a database operation (e.g., `find()`).
5. Assert that the emitted tracing span includes the `db.query.text` attribute.
6. Unset the environment variable `OTEL_#{LANG}_INSTRUMENTATION_MONGODB_QUERY_TEXT_MAX_LENGTH`.
7. Create a new `MongoClient`.
8. Perform the same database operation.
9. Assert that the emitted tracing span does not include the `db.query.text` attribute.

*Test 3: `getMore` inside a `withTransaction` callback nests under the transaction span*

This test covers the convenient transaction API. The core transaction API case is covered by the unified test
[tests/transaction/get_more.yml](transaction/get_more.yml).

This test requires a replica set or a sharded cluster running server version 4.4 or later, matching the existing
convenient transaction API fixture [tests/transaction/convenient.yml](transaction/convenient.yml).

1. Create a `MongoClient` with tracing enabled.
2. Insert three documents into a test collection.
3. Start a session and call `withTransaction`. In the callback, create a cursor over that collection with `find` and a
    `batchSize` of `2`, then iterate the cursor until it is exhausted. This sends exactly one `getMore` inside the
    transaction.
4. Assert that a `transaction` span was emitted, and that both the `find` operation span and the `getMore` operation
    span are nested directly under it.
5. Assert that the `getMore` operation span is a sibling of the `find` operation span, and is not nested under it.

*Test 4: `getMore` records the cursor id it sent, not the cursor id returned*

The unified fixtures assert `db.mongodb.cursor_id: { $$gte: 1 }`, which rules out the `0` the reply carries. This test
asserts the stronger claim that the value equals the id the driver sent, which no matching operator can express.

1. Create a `MongoClient` with tracing enabled.
2. Insert three documents into a test collection.
3. Create a cursor over that collection with `find` and a `batchSize` of `2`. Consume the first batch, then record the
    cursor id before iterating further.
4. Iterate the cursor until it is exhausted. This sends exactly one `getMore`, and the server's reply to that `getMore`
    returns a cursor id of `0`.
5. Assert that both the `getMore` operation span and the `getMore` command span have a `db.mongodb.cursor_id` attribute
    whose value equals the cursor id recorded in step 3.

*Test 5: `error.type` is the exception class name for a non-server error*

The unified fixture [tests/operation/error_type.yml](operation/error_type.yml) asserts that `error.type` is a string on
both the command and operation spans for a non-server error, but matching cannot compare two observed attributes. This
test asserts the value the fixture cannot: `error.type` equals the `exception.type` recorded on the same span, the
exception class name.

1. Create a `MongoClient` with tracing enabled and `retryReads` disabled.
2. Configure a `failCommand` fail point on `find` with `closeConnection: true`.
3. Call `find` on a test collection and let it fail.
4. Assert that the command span's `error.type` attribute equals its `exception.type` attribute.
5. Assert that the operation span's `error.type` attribute equals its `exception.type` attribute.

*Test 6: `error.type` on the operation span is the exception class name for a server error*

The unified fixture's server-error case asserts only that the operation span's `error.type` is a string, since matching
cannot compare one observed attribute against another, or assert that it differs from the command span's server error
code. The specification recommends that the operation span's `error.type` be the exception class name, not the server
error code. This test asserts both.

1. Create a `MongoClient` with tracing enabled.
2. Configure a `failCommand` fail point on `find` with a non-retryable `errorCode`.
3. Call `find` on a test collection and let it fail.
4. Assert that the operation span's `error.type` attribute equals its `exception.type` attribute, and that both differ
    from the `db.response.status_code` attribute on the `find` command span.

#### Server Trace Context Propagation

The following tests verify that servers join the driver's distributed trace (see
[Propagating Trace Context to the Server](../open-telemetry.md#propagating-trace-context-to-the-server)). Server-created
spans are only observable through the server's OpenTelemetry file exporter, which writes spans as OTLP JSON — one export
batch per line (NDJSON) — to a directory on the server's filesystem. There is no wire protocol to retrieve them: the
test runner reads the files directly and therefore must share a filesystem with the server.

These tests require:

- A MongoDB 9.0+ (`maxWireVersion` >= 29) deployment compiled with OpenTelemetry support, started with the OpenTelemetry
    file exporter configured. [drivers-evergreen-tools](https://github.com/mongodb-labs/drivers-evergreen-tools)
    provisions this when orchestration is started with `OTEL=1`, and communicates the trace directory to the test suite
    via the `OTEL_TRACE_DIR` environment variable (see `.evergreen/orchestration/README.md` in that repository). Enable
    `OTEL` only in a dedicated task or variant pinned to a 9.0+ server.
- Skip these tests when `OTEL_TRACE_DIR` is unset or empty. This covers every environment that did not opt in; drivers
    do not need any further environment detection.

Reading server spans:

- Read all files recursively under `OTEL_TRACE_DIR` (each cluster member writes to its own per-port subdirectory). Parse
    each line as an OTLP JSON export batch and collect the spans from `resourceSpans[].scopeSpans[].spans[]`. `traceId`,
    `spanId`, and `parentSpanId` are lowercase hex strings.
- Spans are batched (default flush interval 1000 ms), so poll with a generous timeout (e.g. 30 seconds) rather than
    sleeping once.
- Select only spans whose `traceId` matches a span emitted by the test's own `MongoClient`; the directory may contain
    spans from other tests or from the server's internal sampling.

*Test 7: Server spans join the driver's trace*

1. Create a `MongoClient` with tracing enabled, connected to the deployment described above.
2. Perform an `insertOne` operation on a test collection and record the driver's **command span** for the resulting
    `insert` command (its `traceId` and `spanId`).
3. Poll `OTEL_TRACE_DIR` until a server span appears whose `traceId` equals the command span's `traceId`, or the timeout
    elapses.
4. Assert that exactly one such server span exists for the `insert` command and that its `parentSpanId` equals the
    driver command span's `spanId`.

*Test 8: One server span per retry attempt*

This test uses a retryable read so that it runs on all topologies, including standalone (retryable writes require a
replica set or sharded cluster).

1. Create a `MongoClient` with tracing enabled and retryable reads enabled (the default).

2. Configure the following failpoint (on sharded clusters, configure it on the same `mongos` the client is connected
    to):

    ```javascript
    {
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
            failCommands: ["find"],
            errorCode: 91           // ShutdownInProgress (retryable)
        }
    }
    ```

3. Perform a `find` operation and assert it succeeds. Record the driver's command spans for both `find` attempts (the
    failed first attempt and the successful retry); assert they share one `traceId` and have distinct `spanId` values.

4. Poll `OTEL_TRACE_DIR` for server spans with that `traceId`.

5. Assert that there are exactly two server spans whose `parentSpanId` is one of the two command span `spanId`s, and
    that each attempt's command span has exactly one such child (i.e. the server spans are parented per attempt, not
    both under one span).

6. Disable the failpoint.

*Test 9: No trace context for authentication and monitoring commands*

1. Create a `MongoClient` with tracing enabled against a deployment that requires authentication.
2. Perform a `find` operation on a test collection, so that connection handshakes, authentication (e.g.
    `saslStart`/`saslContinue`), and server monitoring (`hello`) have all occurred.
3. Record the set of `traceId` values of every span emitted by the driver.
4. Poll `OTEL_TRACE_DIR` until the server span for the `find` command appears (this bounds the wait for the remaining
    assertions), then collect all server spans whose `traceId` is in the recorded set.
5. Assert that no collected server span corresponds to an authentication or monitoring command (`hello`, legacy hello,
    `saslStart`, `saslContinue`, `authenticate`): the driver attaches no trace context to those commands, so any server
    spans they produce cannot join the driver's traces.

> [!NOTE]
> The server may create spans for commands the driver did not trace (head-based sampling applies server-side too).
> Assertions are therefore always made on the join between driver `traceId`s and server spans, never on the raw contents
> of the trace directory.
