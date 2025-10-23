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
