# Connection Monitoring and Pooling (CMAP)

______________________________________________________________________

## Introduction

Drivers MUST implement all of the following types of CMAP tests:

- Pool unit and integration tests as described in [cmap-format/README](./cmap-format/README.md)
- Pool prose tests as described below in [Prose Tests](#prose-tests)
- Logging tests as described below in [Logging Tests](#logging-tests)

## Prose Tests

The following tests have not yet been automated, but MUST still be tested:

1. All ConnectionPoolOptions MUST be specified at the MongoClient level
2. All ConnectionPoolOptions MUST be the same for all pools created by a MongoClient
3. A user MUST be able to specify all ConnectionPoolOptions via a URI string
4. A user MUST be able to subscribe to Connection Monitoring Events in a manner idiomatic to their language and driver
5. When a check out attempt fails because connection set up throws an error, assert that a ConnectionCheckOutFailedEvent
    with reason="connectionError" is emitted.

## Logging Tests

Tests for connection pool logging can be found in the `/logging` subdirectory and are written in the
[Unified Test Format](../../unified-test-format/unified-test-format.md).
